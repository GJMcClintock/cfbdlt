# Setting Up a CFB Data Warehouse

## Overview
This repo is a quick and dirty example of using dlt to sync data from the CFB API to a datawarehouse. It uses MotherDuck as the target data warehouse, but you can easily change it to use any other supported target.
## Steps
- Install `uv` by running the following command:
```
macOS/linux:

curl -LsSf https://astral.sh/uv/install.sh | sh

windows:

powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

If you already have a python environment set up, you can run
```
pip install uv
``` 
to install it.

- Create a MotherDuck account, you can do this by going to [MotherDuck](https://motherduck.com/) and signing up. In MotherDuck, create a new database and name it `cfbd`.

- Create a `secrets.toml` file in the `/.dlt` directory. There is already a sample file, you will just need to add your own credentials. The gitignore file will prevent this file from being pushed to the repo.  If you used a name other than `cfbd` for your database, you will need to change the name in this file.


- Run the pipeline!
```
uv run pipeline.py
```

uv will automatically create a virtual environment for you and install the required dependencies then run the file.

## Local Instead
If you don't want to run this to MotherDuck, you can run it locally to a duckdb file. Simply swap `motherduck` for `duckdb` in the pipeline creation statement in `pipeline.py`:

```
pipeline = dlt.pipeline(
    pipeline_name='cfbd_pipeline',
    destination='duckdb',
    dataset_name='raw',
    progress='enlighten'
)
```

This will create a local duckdb file that can be queried using any SQL client.

## Deploying to GitHub
If you want to deploy this to GitHub, you can do so with the following command:
```
dlt deploy pipeline.py github-action --schedule "0 12 * 8-12,1 0"
```
This will walk you through the steps to deploy this to run every Sunday at noon UTC for the months of August through January. You can change the schedule to whatever you want, but this is a good starting point to cover the season and postseason. Make sure you edit the settings file to set the start year to be the current season to avoid pulling a ton of data.

## Notes
- The CFBD API imposes limits based on the tier of support:
    - Free Tier: 1000 requests per month
    - Tier 1 ($1/mo): 5000 requests per month
    - Tier 2 ($5/mo): 30000 requests per month
    - Tier 3 ($10/mo): 75000 requests per month
- There is no active concurrency limit from the API itself, but Cloudflare will block traffic if there are more than 30 requests in a 10s window.
- This script is set to run the pipeline triple threaded to avoid limiting issues. Given a one time backfill is all that is needed, it was not pragmatically significant to worry about the run times.
- After your initial run, you can update the SETTINGS file to only pull the most recent year which will run quickly.
- You can selectively remove tables from the pipeline by commenting them out in the `settings.py` file, or from the `add sources` section of `pipeline.py` for the transformers.
    - `game_advanced_box` and `game_win_probability` will take the longest to run as they are transformed game by game (~3 hours).
    -  Transformers must have their parent included, meaning you need to include `games` and `season_calendar` to run the associated transformers.
- Advanced box scores are sporadic in the earlier years, so I set them to start at the CFP era (2014-) to avoid errors an reduce API calls.
    - There are ~9500 games, so you will need to do this month by month or join Tier 3 on Patreon to get this in one shot.
- The transformers built on the calendar (things that require a year/week parameter) have ~400 up through 2024. Consider this when setting your date range.
- There are a few endpoints (such as player PPA) that are not included because you can calculate them after the fact using SQL.