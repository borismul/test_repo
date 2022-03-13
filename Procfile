web: uvicorn metric_api:app --host=0.0.0.0 --port=${PORT:-5463} & python -c $'from main import *\nrun_event_streamer()'
