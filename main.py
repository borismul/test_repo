from event_streamer.api_caller import DataStreamer
from multiprocessing import Process
import uvicorn
from config import *
import time


def run_event_streamer():
    time.sleep(10)
    ds = DataStreamer()
    ds.run()


if __name__ == "__main__":
    # Process(target=run_event_streamer).start()
    # uvicorn.run("API.api_main:app", host=HOST, port=PORT, reload=True, debug=True)
    run_event_streamer()

