from dlt.sources.helpers import requests
import pandas as pd
from typing import Optional
from settings import ADD_YEAR
from datetime import datetime

def cal_prep(record):
    # Convert calendar_record to a DataFrame and keep only the required columns
    if record is None:
        return None
    df = pd.DataFrame(record)
    
    if not df.empty:
        # Filter records where startDate is less than the current date
        current_date = pd.Timestamp.now()
        df = df[pd.to_datetime(df['startDate'], utc=True).dt.tz_localize(None) < current_date]
        
        filtered_df = df[['season', 'week', 'seasonType']]
        # Rename 'season' column to 'year' to match the expected parameter name
        filtered_df = filtered_df.rename(columns={'season': 'year'})
        return filtered_df

def game_prep(record, start_year):
    # Convert game_record to a DataFrame and keep only the required columns
    if record is None:
        return None
    df = pd.DataFrame(record)
    if not df.empty:
        # Filter games where either home or away team is in FBS classification
        filtered_df = df[
            ((df['homeClassification'].str.lower() == 'fbs') | (df['awayClassification'].str.lower() == 'fbs')) 
            & (df['season'] >= start_year) 
            & (~pd.isna(df['homePoints']))
        ]
        if not filtered_df.empty:
            filtered_df = filtered_df[['id']]
            return filtered_df
    else:
        return None

def get_data(
    url: str, 
    headers: dict, 
    range: pd.DataFrame, 
    endpoint_name: str,
    start_year: Optional[int] = None
    ):
        if start_year is not None:
            # Filter the range DataFrame to include only rows with 'year' >= start_year
            range = range[range['year'] >= start_year]
            # Skip processing if no rows remain after filtering
            if len(range) == 0:
                return
        for _, row in range.iterrows():
            row_params = {}
            # Add each column from the row to the params dict
            for column in range.columns:
                row_params[column] = row[column]
                
            # Make the API request with the combined params
            try:
                response = requests.get(
                    url,
                    params=row_params,
                    headers=headers
                )
                data = response.json()
                if endpoint_name == 'coaches':
                    # Process the coaches data by flattening the seasons and keeping name info
                    flattened_data = []
                    for coach in data:
                        first_name = coach.get('firstName')
                        last_name = coach.get('lastName')
                        hire_date = coach.get('hireDate')
                        
                        # If no seasons, create one record with just the name info
                        if not coach.get('seasons'):
                            flattened_data.append({
                                'firstName': first_name,
                                'lastName': last_name,
                                'hireDate': hire_date
                            })
                        else:
                            # For each season, create a record with season data + name info
                            for season in coach.get('seasons', []):
                                season_data = {
                                    'firstName': first_name,
                                    'lastName': last_name, 
                                    'hireDate': hire_date,
                                    **season  # Unpack all season fields
                                }
                                flattened_data.append(season_data)
                        
                    data = flattened_data  # Replace data with the flattened version
                if endpoint_name in ADD_YEAR:
                    # Add the year to the data
                    for item in data:
                        item['year'] = int(row['year'])
                yield data
            except:
                continue

def get_static(url: str, headers: dict):
    response = requests.get(
        url,
        headers=headers
    )
    data = response.json()
    yield data