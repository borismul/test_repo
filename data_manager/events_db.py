import pandas as pd


class EventsDB:

    db = None

    def __init__(self, desired_events=None):

        self.events = dict()

        if desired_events is None:
            desired_events = ['WatchEvent', 'PullRequestEvent', 'IssuesEvent']

        for event in desired_events:
            self.events[event] = pd.DataFrame()

        self.desired_events = desired_events

        # Add reference to db.
        EventsDB.db = self

    def add_events(self, objects):
        json_list = pd.DataFrame([object.__dict__ for object in objects])
        df = pd.DataFrame().from_records(json_list)
        df['created_at'] = pd.to_datetime(df['created_at'])
        for event in self.desired_events:
            df_event = df[df['type'] == event]

            self.events[event] = pd.concat([self.events[event], df_event], ignore_index=True).drop_duplicates()
