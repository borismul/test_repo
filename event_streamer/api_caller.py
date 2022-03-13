from requests import get, post
from datetime import datetime, timezone
from .exceptions import UnexpectedStatusCodeError
import pandas as pd
from config import *
import base64


class DataStreamer:

    def __init__(self):
        self.events_url = 'https://api.github.com/events'
        self.github_token = base64.b64decode('Z2hwX1FqbEpxQ1VwVG5aWDM4dkptbm9HVmNGYU9EaE5hbzJIcjB0eg=='.encode('ascii')).decode('ascii')
        self.upload_url = f"http://{HOST}:{PORT}/add_events"

    def _get_call(self, token, headers, params):
        resp = get(self.events_url, auth=('borismul', token), headers=headers, params=params)

        if resp.status_code not in [200, 304, 403]:
            print(resp.text)
            raise UnexpectedStatusCodeError(resp.status_code)


        return resp

    def safe_get_call(self, token, headers, params):

        error = True
        max_retries = 5
        retries = 0
        resp = None
        while error and retries <= max_retries:

            try:
                resp = self._get_call(token, headers, params)
                error = False
            except UnexpectedStatusCodeError as e:
                print(e.message)
                print('So, retrying...')
                error = True
                retries += 1

                if retries >= max_retries:
                    raise e

        return resp

    def post_data_to_db(self, data):
        res = post(self.upload_url, data=data)
        return res

    def run(self):
        headers = {'If-None-Match': None}
        params = {'per_page': 100,
                  'page': 1}

        tic = None
        poll_rate = None

        while True:
            toc = datetime.now(timezone.utc)
            # If is NOT first call and the delta time is smaller than the poll rate, delay the next call
            if poll_rate and (toc - tic).total_seconds() <= poll_rate:
                continue

            resp = self.safe_get_call(self.github_token, headers, params)
            etag = resp.headers['etag']
            assert resp.status_code != 403, 'Rate Limit Reached!!!'

            if resp.status_code == 304:
                continue

            tic = datetime.now(timezone.utc)
            result = pd.json_normalize(resp.json())
            result = result[['id', 'type', 'created_at', 'repo.name'] + [x for x in result.columns if 'payload.action' in x]]

            result['repo'] = result['repo.name']
            result['action'] = result['payload.action'].fillna('No Action')
            result = result.drop(columns=['repo.name', 'payload.action'])
            result['created_at'] = pd.to_datetime(result['created_at'])
            poll_rate = 0.8
            headers['If-None-Match'] = etag
            data = result.to_json(orient='records', date_format='iso')
            self.post_data_to_db(data)
            # df = pd.DataFrame().from_records(get('http://127.0.0.1:8000/events').json())
            # print(df.to_string())

# if __name__ == "__main__":
#     DataStreamer().run()