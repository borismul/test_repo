from datastreamer import DataStreamer
import time
from config import *
from multiprocessing import Process
import uvicorn


def run_event_streamer():
    time.sleep(10)
    ds = DataStreamer()
    ds.run()


if __name__ == "__main__":
    Process(target=run_event_streamer).start()
    uvicorn.run("metric_api:app", host=HOST, port=PORT, reload=True, debug=True)

