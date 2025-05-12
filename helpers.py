from dlt.sources.helpers import requests
import pandas as pd
from typing import Optional

def get_data(url: str, headers: dict, range: pd.DataFrame):
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
                
            yield response.json()