import sys
import asyncio
import threading
from bleak import BleakScanner, BleakClient
from bleakheart.bleakheart import PolarMeasurementData
import csv
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def scan():
    """ Scan for a Polar device. """
    device = await BleakScanner.find_device_by_filter(
        lambda dev, adv: dev.name and "polar" in dev.name.lower())
    return device

async def run_ble_client(device, queue, quitclient):
    """ Connect to the BLE server and start ECG notification. """
   
    def disconnected_callback(client):
        """ Called by BleakClient if the sensor disconnects """
        logger.info("Sensor disconnected")
        quitclient.set()  # causes the ble client task to exit

    logger.info(f"Connecting to {device}...")
    try:
        async with BleakClient(device, disconnected_callback=disconnected_callback) as client:
            logger.info(f"Connected: {client.is_connected}")
            await asyncio.sleep(2)  # Add a 2-second delay
            pmd = PolarMeasurementData(client, ecg_queue=queue)
            settings = await pmd.available_settings('ECG')
            logger.info("Request for available ECG settings returned the following:")
            for k, v in settings.items():
                logger.info(f"{k}:\t{v}")
            logger.info(">>> Hit Enter to exit <<<")
            try:
                err_code, err_msg, *_ = await pmd.start_streaming('ECG')
                if err_code != 0:
                    logger.error(f"PMD returned an error: {err_msg}")
                    sys.exit(err_code)
            except Exception as e:
                logger.error(f"Error starting ECG stream: {e}")
                sys.exit(-1)
           
            await quitclient.wait()
            if client.is_connected:
                await pmd.stop_streaming('ECG')
            queue.put_nowait(('QUIT', None, None, None))
    except Exception as e:
        logger.error(f"Error in run_ble_client: {e}")
        quitclient.set()

async def run_consumer_task_ecg(queue):
    """ Retrieve ECG data from the queue and store it in a CSV file. """
    logger.info("After connecting, will print ECG data in the form")
    logger.info("('ECG', tstamp, [s1, S2, ..., sn])")
    with open('ecg_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Timestamp (ns)', 'ECG Sample (microVolt)'])
        while True:
            frame = await queue.get()
            if frame[0] == 'QUIT':
                break
            timestamp, ecg_data = frame[1], frame[2]
            for i, sample in enumerate(ecg_data):
                if i == 0:
                    writer.writerow([timestamp, sample])
                else:
                    writer.writerow(['', sample])
            logger.info(frame)

def input_thread(quitclient):
    input()
    quitclient.set()
    logger.info("Quitting on user command")

async def main():
    logger.info("Scanning for BLE devices")
    device = await scan()
    if device is None:
        logger.error("Polar device not found.")
        sys.exit(-4)
    ecgqueue = asyncio.Queue()
    quitclient = asyncio.Event()
    input_thread_obj = threading.Thread(target=input_thread, args=(quitclient,))
    input_thread_obj.start()
    producer = run_ble_client(device, ecgqueue, quitclient)
    consumer = run_consumer_task_ecg(ecgqueue)
    await asyncio.gather(producer, consumer)
    logger.info("Bye.")
    input_thread_obj.join()

if __name__ == "__main__":
    asyncio.run(main())