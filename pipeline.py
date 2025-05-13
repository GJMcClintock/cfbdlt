import dlt
from settings import *
from helpers import get_data, get_static, cal_prep, game_prep
import pandas as pd

# Create dataframe with years from settings.py
YEARS = pd.DataFrame({'year': range(START_YEAR, END_YEAR + 1)})


# a dlt "source" decorator to define the functions
# a source can have resources (base tables) or transformers (dependent tables)
# This is an example of using a source that yields a dlt resource
# rather than definining the resource in a function.
# this takes the lists of endpoints and creates a resource for each.
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
        start_year = endpoint[3]
        url = f"{BASE_URL}/{endpoint_path}"
        yield dlt.resource(
            get_data,
            name=endpoint_name,
            write_disposition='merge',
            merge_key=merge_key_set,
            parallelized=True
        )(url, headers=headers, range=YEARS,endpoint_name=endpoint_name, start_year=start_year)

    for endpoint in STATIC_ENDPOINTS:
        endpoint_name = endpoint[0]
        endpoint_path = endpoint[1]
        url = f"{BASE_URL}/{endpoint_path}"
        yield dlt.resource(
            get_static,
            name=endpoint_name,
            write_disposition='replace',
            parallelized=True
        )(url, headers=headers)

# The rest of our functions are "transformers" meaning
# they take the output of a resource and feed it into another call.
# Transformers cannot be dynamically generated like resources.
@dlt.transformer(
    name='plays',
    write_disposition='merge',
    merge_key=('id'),
    parallelized=True
)
def plays(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }

    url = f"{BASE_URL}/plays"
    if range is not None:
        range = range[range['year'] >= 2001]
        # Skip processing if no rows remain after filtering
        if len(range) == 0:
            return
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='plays'
        )

@dlt.transformer(
    name='team_box_score',
    write_disposition='merge',
    merge_key=('id'),
    parallelized=True
)
def team_box_score(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/games/teams"
    if range is not None:
        range = range[range['year'] >= 2004]
        # Skip processing if no rows remain after filtering
        if len(range) == 0:
            return
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='team_box_score'
        )

@dlt.transformer(
    name='player_box_score',
    write_disposition='merge',
    merge_key=('id'),
    parallelized=True
)
def player_box_score(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/games/players"
    if range is not None:
        range = range[range['year'] >= 2004]
        # Skip processing if no rows remain after filtering
        if len(range) == 0:
            return
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='player_box_score'
        )

@dlt.transformer(
    name='play_stats',
    write_disposition='merge',
    merge_key=('play_id','stat_type','athlete_id'),
    parallelized=True
)
def play_stats(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/plays/stats"
    if range is not None:
        range = range[range['year'] >= 2013]
        # Skip processing if no rows remain after filtering
        if len(range) == 0:
            return
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='play_stats'
        )

@dlt.transformer(
    name='lines',
    write_disposition='merge',
    merge_key=('id'),
    parallelized=True
)
def lines(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/lines"
    if range is not None:
        range = range[range['year'] >= 2013]
        # Skip processing if no rows remain after filtering
        if len(range) == 0:
            return
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='lines'
        )

@dlt.transformer(
    name='game_advanced_stats',
    write_disposition='merge',
    merge_key=('game_id'),
    parallelized=True
)
def game_advanced_stats(calendar_record,
          cfbd_api_key: str = dlt.secrets.value):
    range = cal_prep(calendar_record)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/stats/game/advanced"
    if range is not None:
        range = range[range['year'] >= 2002]
        # Skip processing if no rows remain after filtering
        if len(range) == 0:
            return
        yield get_data(
            url,
            headers=headers,
            range=range,
            endpoint_name='game_advanced_stats'
        )

@dlt.transformer(
    name='game_advanced_box',
    write_disposition='merge',
    merge_key=('game_id'),
    parallelized=True
)
def game_advanced_box(game_record,
          cfbd_api_key: str = dlt.secrets.value):
    games = game_prep(game_record, 2014)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/game/box/advanced"
    if games is not None:
        data = get_data(
            url,
            headers=headers,
            range=games,
            endpoint_name='game_advanced_stats'
        )
        yield data

@dlt.transformer(
    name='game_win_probability',
    write_disposition='merge',
    merge_key=('game_id','play_number'),
    parallelized=True
)
def game_win_probability(game_record,
          cfbd_api_key: str = dlt.secrets.value):
    games = game_prep(game_record, 2014)
    headers = {
        'authorization': f'Bearer {cfbd_api_key}'
    }
    url = f"{BASE_URL}/metrics/wp"
    if games is not None:
        # Rename 'id' column to 'gameId' in the games dataframe
        games = games.rename(columns={'id': 'gameId'})
        data = get_data(
            url,
            headers=headers,
            range=games,
            endpoint_name='game_win_probability'
        )
        yield data


# Creating a source object, we can add new resources or transformers
# to it. Here we add the transformers created above with the syntax
# source.<parent_resource> | <transformer_name>
source =  cfbd_source()
source.resources.add(
    # source.season_calendar | plays,
    # source.season_calendar | team_box_score,
    # source.season_calendar | player_box_score,
    # source.season_calendar | play_stats,
    # source.season_calendar | lines,
    source.season_calendar | game_advanced_stats
    # source.games | game_win_probability,
    # source.games | game_advanced_box
)

# Creating a pipeline instance and run it with the source.
pipeline = dlt.pipeline(
    pipeline_name='cfbd_pipeline',
    destination='motherduck',
    dataset_name='raw',
    progress='enlighten'
)

load_info = pipeline.run(source)
print(load_info)