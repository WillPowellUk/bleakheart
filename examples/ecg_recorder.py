import sys
import asyncio
from threading import Thread
from bleak import BleakScanner, BleakClient

# Allow importing bleakheart from parent directory
sys.path.append('../')
from bleakheart import PolarMeasurementData


def start_keyboard_listener(event):
    """Listens for Enter key press on a separate thread to stop the asyncio loop."""
    input(">>> Hit Enter to exit <<<")
    event.set()


async def scan():
    """ Scan for a Polar device. """
    device = await BleakScanner.find_device_by_filter(
        lambda dev, adv: dev.name and "polar" in dev.name.lower())
    return device


async def run_ble_client(device, queue):
    """ This task connects to the BLE server (the heart rate sensor)
    identified by device, starts ECG notification and pushes the ECG 
    data to the queue. The tasks terminates when the sensor disconnects 
    or the user hits enter. """

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

        # create the Polar Measurement Data object; set queue for ecg data
        pmd = PolarMeasurementData(client, ecg_queue=queue)

        # ask about ACC settings
        settings = await pmd.available_settings('ECG')
        print("Request for available ECG settings returned the following:")
        for k, v in settings.items():
            print(f"{k}:\t{v}")

        # start notifications; bleakheart will start pushing data to the queue we passed to PolarMeasurementData
        (err_code, err_msg, _) = await pmd.start_streaming('ECG')
        if err_code != 0:
            print(f"PMD returned an error: {err_msg}")
            sys.exit(err_code)

        # this task does not need to do anything else; wait until user hits enter or the sensor disconnects
        await quitclient.wait()

        # no need to stop notifications if we are exiting the context manager anyway, as they will disconnect the client; however, it's easy to stop them if we want to
        if client.is_connected:
            await pmd.stop_streaming('ECG')

        # signal the consumer task to quit
        queue.put_nowait(('QUIT', None, None, None))


async def run_consumer_task(queue):
    """ This task retrieves ECG data from the queue and does all the processing. """
    print("After connecting, will print ECG data in the form")
    print("('ECG', tstamp, [s1,S2,...,sn])")
    print("where samples s1,...sn are in microVolt, tstamp is in ns")
    print("and it refers to the last sample sn.")
    while True:
        frame = await queue.get()
        if frame[0] == 'QUIT':  # intercept exit signal
            break
        print(frame)


async def main():
    print("Scanning for BLE devices")
    device = await scan()
    if device is None:
        print("Polar device not found.")
        sys.exit(-4)
    
    ecgqueue = asyncio.Queue()
    producer = run_ble_client(device, ecgqueue)
    consumer = run_consumer_task(ecgqueue)
    await asyncio.gather(producer, consumer)
    print("Bye.")


# execute the main coroutine
asyncio.run(main())
