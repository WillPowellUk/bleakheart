import sys
import asyncio
from threading import Thread
from bleak import BleakScanner, BleakClient
import csv

# Allow importing bleakheart from parent directory
sys.path.append('../')
from bleakheart import HeartRate

# Change these two parameters and see what happens.
# INSTANT_RATE is unsupported when UNPACK is False
UNPACK = True
INSTANT_RATE = UNPACK and True


def start_keyboard_listener(event):
    """Listens for Enter key press on a separate thread to stop the asyncio loop."""
    input(">>> Hit Enter to exit <<<")
    event.set()


async def scan():
    """ Scan for a Polar device. If you have another compatible device,
    edit the string in the code below accordingly """
    device = await BleakScanner.find_device_by_filter(
        lambda dev, adv: dev.name and "polar" in dev.name.lower())
    return device


async def run_ble_client(device, hrqueue):
    """ This task connects to the BLE server (the heart rate sensor)
    identified by device, starts heart rate notification and pushes 
    heart rate data to hrqueue. The tasks terminates when the sensor 
    disconnects or the user hits enter. """

    def disconnected_callback(client):
        """ Called by BleakClient if the sensor disconnects """
        print("Sensor disconnected")
        quitclient.set()  # causes the ble client task to exit

    # we use this event to signal the end of the client task
    quitclient = asyncio.Event()
    print(f"Connecting to {device}...")
    # the context manager will handle connection/disconnection for us
    async with BleakClient(device, disconnected_callback=disconnected_callback) as client:
        print(f"Connected: {client.is_connected}")

        # Start the keyboard listener thread
        Thread(target=start_keyboard_listener, args=(quitclient,), daemon=True).start()

        # Create the heart rate object; set queue and other parameters
        heartrate = HeartRate(client, queue=hrqueue, instant_rate=INSTANT_RATE, unpack=UNPACK)

        # Start notifications; bleakheart will start pushing data to the queue
        await heartrate.start_notify()

        # This task does not need to do anything else; wait until user hits enter or the sensor disconnects
        await quitclient.wait()

        # No need to stop notifications if we are exiting the context manager anyway, as they will disconnect the client; however, it's easy to stop them if we want to
        if client.is_connected:
            await heartrate.stop_notify()

        # Signal the consumer task to quit
        hrqueue.put_nowait(('QUIT', None, None, None))


async def run_consumer_task(hrqueue):
    """ This task retrieves heart rate data from the queue, processes it,
    and writes it to a CSV file. """
    with open('ecg_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        # Write the header to the CSV file
        writer.writerow(['Timestamp', 'Heart Rate', 'RR Intervals', 'Energy Expenditure'])

        while True:
            frame = await hrqueue.get()
            if frame[0] == 'QUIT':  # intercept exit signal
                break
            # Assuming frame structure is ('HR', timestamp, (bpm, [rr1,rr2,...]), energy)
            timestamp, heart_rate_data, energy = frame[1], frame[2], frame[3]
            bpm, rr_intervals = heart_rate_data[0], heart_rate_data[1]
            # Convert rr_intervals to a string if it's a list for CSV compatibility
            rr_intervals_str = ','.join(map(str, rr_intervals)) if isinstance(rr_intervals, list) else rr_intervals
            writer.writerow([timestamp, bpm, rr_intervals_str, energy])
            print(frame)


async def main():
    device = await scan()
    if device is None:
        print("Polar device not found. If you have another compatible device, edit the scan() function accordingly.")
        sys.exit(-4)

    hrqueue = asyncio.Queue()
    producer = run_ble_client(device, hrqueue)
    consumer = run_consumer_task(hrqueue)
    await asyncio.gather(producer, consumer)
    print("Bye.")


# Execute the main coroutine
asyncio.run(main())
