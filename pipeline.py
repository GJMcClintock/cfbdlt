import dlt
from settings import *
from helpers import get_data, get_static, cal_prep
import pandas as pd

# Create dataframe with years
YEARS = pd.DataFrame({'year': range(START_YEAR, END_YEAR + 1)})

@dlt.source(
    name='cfbd'
)
def cfbd_source(
    cfbd_api_key: str = dlt.secrets.value,
    ):

    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }

    for endpoint in SEASON_ENDPOINTS:
        endpoint_name = endpoint[0]
        endpoint_path = endpoint[1]
        merge_key_set = endpoint[2]
        url = f"{BASE_URL}/{endpoint_path}"
        yield dlt.resource(
            get_data,
            name=endpoint_name,
            write_disposition='merge',
            merge_key=merge_key_set,
            parallelized=False
        )(url, headers=headers, range=YEARS,endpoint_name=endpoint_name)

    for endpoint in STATIC_ENDPOINTS:
        endpoint_name = endpoint[0]
        endpoint_path = endpoint[1]
        url = f"{BASE_URL}/{endpoint_path}"
        yield dlt.resource(
            get_static,
            name=endpoint_name,
            write_disposition='replace'
        )(url, headers=headers)

@dlt.transformer
def plays(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    # Convert calendar_record to a DataFrame and keep only the required columns
    df = pd.DataFrame(calendar_record)
    filtered_df = df[['season', 'week', 'seasonType']]
    # Rename 'season' column to 'year' to match the expected parameter name
    filtered_df = filtered_df.rename(columns={'season': 'year'})
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/plays"
    yield get_data(
        url,
        headers=headers,
        range=filtered_df,
        endpoint_name='plays'
    )

@dlt.transformer(
    name='plays',
    write_disposition='merge',
    merge_key=('id')
)
def plays(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }

    url = f"{BASE_URL}/plays"
    if range is not None:
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='plays'
        )

@dlt.transformer(
    name='team_box_score',
    write_disposition='merge',
    merge_key=('id')
)
def team_box_score(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/games/teams"
    if range is not None:
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='team_box_score'
        )

@dlt.transformer(
    name='player_box_score',
    write_disposition='merge',
    merge_key=('id')
)
def player_box_score(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/games/players"
    if range is not None:
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='player_box_score'
        )

@dlt.transformer(
    name='play_stats',
    write_disposition='merge',
    merge_key=('play_id','stat_type','athlete_id')
)
def play_stats(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/plays/stats"
    if range is not None:
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='play_stats'
        )

@dlt.transformer(
    name='lines',
    write_disposition='merge',
    merge_key=('id')
)
def lines(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/lines"
    if range is not None:
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='lines'
        )



source =  cfbd_source()
source.resources.add(source.season_calendar | plays,
                     source.season_calendar | team_box_score,
                     source.season_calendar | player_box_score,
                     source.season_calendar | play_stats,
                     source.season_calendar | lines)

pipeline = dlt.pipeline(
    pipeline_name='cfbd_pipeline',
    destination='motherduck',
    dataset_name='raw',
    progress='enlighten'
)

load_info = pipeline.run(source)
print(load_info)