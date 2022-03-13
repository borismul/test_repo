from datetime import timedelta
from io import BytesIO
from typing import List, Optional

import matplotlib as mpl
import pandas as pd
from fastapi import FastAPI, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from data_manager.events_db import EventsDB

# This is necessary for plotting from non-main threads
mpl.use('Agg')
import matplotlib.pyplot as plt
from enum import Enum

tags_metadata = [
    {
        "name": "add_events",
        "description": "Add new events to the database. This is supposed to be for internal use of the Python program "
                       "only.",
    },
    {
        "name": "get_events",
        "description": "Returns all or events of a specific type or specific event. Will be slow with a large number "
                       "of events. Should ideally contain paging..."
    },
    {
        "name": "avg_time_between_pull_requests",
        "description": "Returns average time between pull requests from a specific repo."
    },
    {
        "name": "event_count",
        "description": "Returns the event count."
    },
    {
        "name": "activity_over_time",
        "description": "Returns a graph for the event count over time."
    },

]

db = EventsDB()
app = FastAPI(
    title='Github activity metrics API',
    description='API with some metrics on activity events provided by the github REST API.',
    docs_url='/',
    openapi_tags=tags_metadata
)


class AggFreq(str, Enum):
    hourly = "H"
    minutely = 'T'
    secondly = 'S'


class EventNames(str, Enum):
    WatchEvent = "WatchEvent"
    PullRequestEvent = 'PullRequestEvent'
    IssuesEvent = 'IssuesEvent'


class Event(BaseModel):
    id: int
    type: str
    created_at: str
    repo: str
    action: str


@app.post("/add_events", tags=['add_events'])
def add_events(events: List[Event]):
    db.add_events(events)
    return events


@app.get("/events", tags=['get_events'])
def get_events(event_type: Optional[EventNames] = Query(None,
                                                        description="Return only specific events. Leave empty for all "
                                                                    "events. Can be: 'WatchEvent', 'PullRequestEvent' "
                                                                    "or 'IssuesEvent'"),
               repo_name: str = Query(None, description='Return only events from a specific repo.')):
    # Select specific events if desired.
    if event_type is None:
        df = pd.concat(db.events)
    else:
        df = db.events[event_type]

    if repo_name is not None:
        df = df[df['repo'] == repo_name]

    return df.to_dict('records')


@app.get("/avg_time_between_pull_requests", tags=['avg_time_between_pull_requests'])
def avg_time_between_pull_requests(
        repo_name: str = Query(None, description='Get the average time in seconds between pull requests for this repo name.')):
    # Obtain opened pull requests from specific repo
    df = db.events['PullRequestEvent']
    df_repo = df[(df['repo'] == repo_name) & (df['action'] == 'opened')]

    # Compute the mean of the timestamp diff.
    mean_time = df_repo['created_at'].diff().mean()

    if str(mean_time) == 'NaT':
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail='This repo does not have at least 2 pull request '
                                                                'events since starting the API. ' \
                                                                'To compute a mean seconds between pull requests, a '
                                                                'minimum of 2 PullRequests events is required.')
    else:
        mean_time = mean_time.total_seconds()
    return mean_time


@app.get("/event_count", tags=['event_count'])
def get_event_count(offset_minutes: Optional[float] = Query(None,
                                                            description='Return the count from offset_minutes before '
                                                                        'the last event until the last event (Events '
                                                                        'are always delayed 5 minutes). Leave empty '
                                                                        'for all time. Should be bigger than 0.',
                                                            gt=0)):
    # Concatenate all events
    df_all = pd.concat(db.events)

    # Filter for provided offset.
    if offset_minutes is not None:
        df_all = df_all[df_all['created_at'] > df_all['created_at'].max() - timedelta(minutes=float(offset_minutes))]

    # Compute the count per event in selected interval
    result = df_all.groupby('type').count()['id']
    return result.to_dict()


@app.get("/activity_over_time", tags=['activity_over_time'])
def acivity_over_time(offset_minutes: Optional[float] = Query(None,
                                                              description='Plot data from offset_minutes before the '
                                                                          'last event until the last event (Events are'
                                                                          ' always delayed 5 minutes). Leave empty for'
                                                                          ' data from all time. Should be bigger '
                                                                          'than 0',
                                                              gt=0),
                      aggregation_frequency: AggFreq = Query(AggFreq.minutely,
                                                             description='Count data per frequency. Can be H for '
                                                                         'hourly, T for minutely or S for secondly.')):
    # Get all event dataframes
    dfs = db.events.values()

    # Intialise a list for x data, y data and start the plot
    results_x = list()
    results_y = list()
    fig, ax = plt.subplots()

    # Loop over the dfs and fill the x and y data
    for df in dfs:

        # Filter for specific offset
        if offset_minutes is not None:
            df = df[df['created_at'] > df['created_at'].max() - timedelta(minutes=float(offset_minutes))]

        # Apply frequency
        df['created_at_minutes'] = df['created_at'].dt.floor(aggregation_frequency)
        res = df.groupby('created_at_minutes').count()['id'].reset_index()

        # Store result
        results_x.append(res['created_at_minutes'])
        results_y.append(res['id'])

    # Plot the data
    for res_x, res_y in zip(results_x, results_y):
        ax.plot(res_x, res_y)

    # Apply formatting to plot
    ax.set_ylabel('Event count')
    ax.legend(db.events.keys())
    ax.set_title('Github activity event counts')
    fig.tight_layout()

    # save as a bytes stream.
    buf = BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)

    return StreamingResponse(buf, media_type="image/png")
