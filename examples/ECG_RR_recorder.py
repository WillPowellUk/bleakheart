import sys
import asyncio
import csv
from bleak import BleakScanner, BleakClient
sys.path.append('../')  # Adjust as needed
from bleakheart import PolarMeasurementData, HeartRate

async def scan():
    """ Scan for a Polar device. """
    return await BleakScanner.find_device_by_filter(
        lambda dev, adv: dev.name and "polar" in dev.name.lower())

async def run_ble_client(client, queue, data_type):
    """ Start notifications based on data type and push data to queue """
    try:
        print(f"Streaming {data_type}")

        if data_type == 'ECG':
            pmd = PolarMeasurementData(client, ecg_queue=queue)
            await pmd.start_streaming('ECG')
        elif data_type == 'HR':
            heartrate = HeartRate(client, queue=queue, instant_rate=True, unpack=True)
            await heartrate.start_notify()

        await asyncio.Event().wait()  # Wait indefinitely until cancelled

    except asyncio.CancelledError:
        print(f"Stopping {data_type} data stream")
        if data_type == 'ECG':
            await pmd.stop_streaming('ECG')
        elif data_type == 'HR':
            await heartrate.stop_notify()
        raise

async def run_consumer_task(queue):
    """ Handles both ECG and HR data """
    with open('combined_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Timestamp (ns)', 'ECG Sample (microVolt)', 'RR Interval (ms)'])
        timestamp_flag = True
        while True:
            frame = await queue.get()
            if frame[0] == 'QUIT':
                break
            data_type = frame[0]
            if timestamp_flag:
                timestamp_flag = False
                timestamp_start = frame[1]
                
        
            if data_type == 'HR':
                data_type, timestamp, RR_data, _ = frame
                print(f"Received {data_type} data: {RR_data}")
                # only write RR rate - ignore BPM
                writer.writerow([timestamp-timestamp_start, '', RR_data[1]])
            elif data_type == 'ECG':
                data_type, timestamp, ecg_data = frame
                print(f"Received {data_type} data: {ecg_data}")
                writer.writerow([timestamp-timestamp_start, ecg_data, ''])

async def main():
    device = await scan()
    if device is None:
        print("Compatible Polar device not found.")
        sys.exit(-4)

    client = BleakClient(device)
    await client.connect()
    print(f"Connected to {device}")

    queue = asyncio.Queue()
    producer_ecg = asyncio.create_task(run_ble_client(client, queue, 'ECG'))
    producer_hr = asyncio.create_task(run_ble_client(client, queue, 'HR'))
    consumer = run_consumer_task(queue)

    try:
        await asyncio.gather(producer_ecg, producer_hr, consumer)
    except asyncio.CancelledError:
        print("Tasks cancelled")
    finally:
        await client.disconnect()

asyncio.run(main())
