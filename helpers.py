from dlt.sources.helpers import requests
import pandas as pd
from typing import Optional
from settings import ADD_YEAR

def cal_prep(record):
    # Convert calendar_record to a DataFrame and keep only the required columns
    if record is None:
        return None
    df = pd.DataFrame(record)
    if not df.empty:
        filtered_df = df[['season', 'week', 'seasonType']]
        # Rename 'season' column to 'year' to match the expected parameter name
        filtered_df = filtered_df.rename(columns={'season': 'year'})
        return filtered_df

def get_data(url: str, headers: dict, range: pd.DataFrame, endpoint_name: str):
        for _, row in range.iterrows():
            row_params = {}
            # Add each column from the row to the params dict
            for column in range.columns:
                row_params[column] = row[column]
                
            # Make the API request with the combined params
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

def get_static(url: str, headers: dict):
    response = requests.get(
        url,
        headers=headers
    )
    data = response.json()
    yield data