from datastreamer import DataStreamer
import time
from multiprocessing import Process
import uvicorn
from config import HOST, PORT

def run_event_streamer():
    time.sleep(1)
    ds = DataStreamer()
    ds.run()


if __name__ == "__main__":
    Process(target=run_event_streamer).start()
    uvicorn.run("metric_api:app", host=HOST, port=PORT, reload=True, debug=True)

