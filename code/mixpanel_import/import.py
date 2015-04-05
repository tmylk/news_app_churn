import requests
import hashlib
import time
import json
import pandas as pd
from sqlalchemy import create_engine

# fetches events from mixpanel and stores them in db and csv
# Based on official mixpanel python library
# https://mixpanel.com/site_media/api/v2/mixpanel.py

ENDPOINT = 'https://data.mixpanel.com/api'
VERSION = '2.0'


def hash_args(args, secret):
    """
        Hashes arguments by joining key=value pairs, appending a secret, and
        then taking the MD5 hex digest.
    """
    for a in args:
        if isinstance(args[a], list):
            args[a] = json.dumps(args[a])

    args_joined = ''
    for a in sorted(args.keys()):
        if isinstance(a, unicode):
            args_joined += a.encode('utf-8')
        else:
            args_joined += str(a)

        args_joined += '='

        if isinstance(args[a], unicode):
            args_joined += args[a].encode('utf-8')
        else:
            args_joined += str(args[a])

    hash = hashlib.md5(args_joined)

    if secret:
        hash.update(secret)
    elif api_secret:
        hash.update(api_secret)
    return hash.hexdigest()


def send_export_request(time_for_request, from_date, to_date,
                        event=None, where=None, bucket=None,
                        api_key='**************',
                        api_secret='***********', timeout=30
                        ):
    params = {'from_date': from_date, 'to_date': to_date}
    params['api_key'] = api_key
    # Grant this request 10 minutes.
    params['expire'] = int(time.time()) + time_for_request
    if 'sig' in params:
        del params['sig']
    params['sig'] = hash_args(params, api_secret)

    request_url = ENDPOINT + '/' + VERSION + '/' + 'export'
    return requests.get(request_url, params=params, timeout=timeout)


def _export_to_df(data, columns=None, exclude_mp=False):
    # Keep track of the parameters each returned event
    parameters = set('event')

    # Calls to the data export API do not return JSON.  They return
    # records separated by newlines, where each record is valid JSON.
    # The event parameters are in the properties field
    events = []
    for line in data.split('\n'):
        try:
            event = json.loads(line)
            props = event['properties']
            props['event_type'] = event['event']

        except ValueError:  # Not valid JSON
            continue

        parameters.update(props.keys())
        events.append(props)

    # If columns is excluded, leave off parameters that start with '$' as
    # these are automatically included in the Mixpanel events and clutter the
    # real data
    if columns is None:
        if exclude_mp:
            columns = [p for p in parameters if not (p.startswith('$') or
                                                     p.startswith('mp_'))]
        else:
            columns = parameters
    elif 'time' not in columns:
        columns.append('time')
    df = pd.DataFrame(events, columns=columns)

    # Make time a datetime.
    df['timestamp'] = df['time']
    df['time'] = df['time'].map(lambda x: datetime.datetime.fromtimestamp(x))

    return df


def from_mp_to_disk(date, time_for_request):
    r = send_export_request(time_for_request, date, date)
    df = _export_to_df(r.text)
    print date, df.shape
    df.to_csv('mp_' + date + '.csv', encoding='utf-8')


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


# time in seconds that mixpanel should taek to complete my request
# I can't make any more requests until it expired
time_for_request = 600
total_df = pd.DataFrame()
for single_date in daterange(start_date, end_date):
    date = single_date.strftime("%Y-%m-%d")
    print 'Fetching events for', date
    small_df = from_mp_to_df(date, time_for_request)
    # small_df.to_sql(date+'raw',engine,if_exists='replace')
    total_df = total_df.append(small_df)
    print 'Total events so far', total_df.shape

engine = create_engine('postgresql://root:mp@localhost:5432/mixpanel')
total_df.to_sql('raw', engine, if_exists='replace')
