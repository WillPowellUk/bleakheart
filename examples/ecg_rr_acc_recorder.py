import sys
import asyncio
import threading
from bleak import BleakScanner, BleakClient
from bleakheart.bleakheart import PolarMeasurementData, HeartRate
import csv
import os
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

UNPACK = True
INSTANT_RATE = UNPACK and True

async def scan():
    """ Scan for a Polar device. """
    device = await BleakScanner.find_device_by_filter(
        lambda dev, adv: dev.name and "polar" in dev.name.lower())
    return device

async def run_ble_client(device, ecg_queue, hr_queue, acc_queue, quitclient):
    """ Connect to the BLE server and start ECG, HR, and ACC notifications. """
   
    def disconnected_callback(client):
        """ Called by BleakClient if the sensor disconnects """
        logger.info("Sensor disconnected")
        quitclient.set()  # causes the ble client task to exit

    logger.info(f"Connecting to {device}...")
    try:
        async with BleakClient(device, disconnected_callback=disconnected_callback) as client:
            logger.info(f"Connected: {client.is_connected}")
            await asyncio.sleep(2)  # Add a 2-second delay

            # Start ECG notification
            pmd = PolarMeasurementData(client, ecg_queue=ecg_queue)
            settings = await pmd.available_settings('ECG')
            logger.info("Request for available ECG settings returned the following:")
            for k, v in settings.items():
                logger.info(f"{k}:\t{v}")
            try:
                err_code, err_msg, *_ = await pmd.start_streaming('ECG')
                if err_code != 0:
                    logger.error(f"PMD returned an error: {err_msg}")
                    sys.exit(err_code)
            except Exception as e:
                logger.error(f"Error starting ECG stream: {e}")
                sys.exit(-1)

            # Start HR notification
            heartrate = HeartRate(client, queue=hr_queue, instant_rate=INSTANT_RATE, unpack=UNPACK)
            await heartrate.start_notify()

            # Start ACC notification
            acc_pmd = PolarMeasurementData(client, callback=lambda data: acc_queue.put_nowait(data))
            settings = await acc_pmd.available_settings('ACC')
            logger.info("Request for available ACC settings returned the following:")
            for k, v in settings.items():
                logger.info(f"{k}:\t{v}")
            try:
                err_code, err_msg, *_ = await acc_pmd.start_streaming('ACC', RANGE=2, SAMPLE_RATE=25)
                if err_code != 0:
                    logger.error(f"PMD returned an error: {err_msg}")
                    sys.exit(err_code)
            except Exception as e:
                logger.error(f"Error starting ACC stream: {e}")
                sys.exit(-1)

            await quitclient.wait()

            # Stop streaming and notifications if still connected
            if client.is_connected:
                await pmd.stop_streaming('ECG')
                await heartrate.stop_notify()
                await acc_pmd.stop_streaming('ACC')
            
            ecg_queue.put_nowait(('QUIT', None, None, None))
            hr_queue.put_nowait(('QUIT', None, None, None))
            acc_queue.put_nowait(('QUIT', None, None, None))
    except Exception as e:
        logger.error(f"Error in run_ble_client: {e}")
        quitclient.set()

async def run_consumer_task_ecg(ecg_queue, file_path):
    """Retrieve ECG data from the queue and store it in CSV files."""

    # Ensure the directory exists
    os.makedirs(file_path, exist_ok=True)
    
    # Define file paths for ECG data
    file_path_ecg = os.path.join(file_path, 'ECG.csv')

    # Open the file for writing
    with open(file_path_ecg, 'w', newline='') as ecg_file:
        ecg_writer = csv.writer(ecg_file)
        
        while True:
            frame = await ecg_queue.get()
            if frame[0] == 'QUIT':
                break

            timestamp_data, ecg_data = frame[1], frame[2]
            
            # Write each ECG sample to ECG.csv
            for sample in ecg_data:
                ecg_writer.writerow([sample])

            logger.info(frame)

async def run_consumer_task_hr(hr_queue, file_path):
    """Retrieve HR data from the queue and store it in a CSV file."""

    # Define file path for HR data
    file_path_hr = os.path.join(file_path, 'HR.csv')

    # Open the file for writing
    with open(file_path_hr, 'w', newline='') as hr_file:
        hr_writer = csv.writer(hr_file)

        while True:
            frame = await hr_queue.get()
            if frame[0] == 'QUIT':  # intercept exit signal
                break

            timestamp, heart_rate_data, energy = frame[1], frame[2], frame[3]
            bpm, rr_intervals = heart_rate_data[0], heart_rate_data[1]
            rr_intervals_str = ','.join(map(str, rr_intervals)) if isinstance(rr_intervals, list) else rr_intervals
            hr_writer.writerow([rr_intervals_str])
            logger.info(frame)

async def run_consumer_task_acc(acc_queue, file_path):
    """Retrieve ACC data from the queue and store it in a CSV file."""

    # Define file path for ACC data
    file_path_acc = os.path.join(file_path, 'ACC.csv')

    # Open the file for writing
    with open(file_path_acc, 'w', newline='') as acc_file:
        acc_writer = csv.writer(acc_file)

        while True:
            frame = await acc_queue.get()
            if frame[0] == 'QUIT':  # intercept exit signal
                break
            
            if frame[0] != 'ACC':
                logger.error(f"Unexpected frame type: {frame[0]}")
                continue

            tstamp, acc_data = frame[1], frame[2]
            for sample in acc_data:
                acc_writer.writerow([tstamp, *sample])
            logger.info(frame)

def input_thread(quitclient):
    input(">>> Hit Enter to exit <<<")
    quitclient.set()
    logger.info("Quitting on user command")

async def main(file_path):
    logger.info("Scanning for BLE devices")
    device = await scan()
    if device is None:
        logger.error("Polar device not found.")
        sys.exit(-4)
    ecg_queue = asyncio.Queue()
    hr_queue = asyncio.Queue()
    acc_queue = asyncio.Queue()
    quitclient = asyncio.Event()
    input_thread_obj = threading.Thread(target=input_thread, args=(quitclient,))
    input_thread_obj.start()
    producer = run_ble_client(device, ecg_queue, hr_queue, acc_queue, quitclient)
    consumer_ecg = run_consumer_task_ecg(ecg_queue, file_path)
    consumer_hr = run_consumer_task_hr(hr_queue, file_path)
    consumer_acc = run_consumer_task_acc(acc_queue, file_path)
    await asyncio.gather(producer, consumer_ecg, consumer_hr, consumer_acc)
    logger.info("Bye.")
    input_thread_obj.join()

if __name__ == "__main__":
    subject = 1
    file_name = f'data_collection/recordings/S{subject}'
    asyncio.run(main(file_name))
