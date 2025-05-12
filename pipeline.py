import dlt
from settings import *
from helpers import get_data
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

    print(headers)
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


pipeline = dlt.pipeline(
    pipeline_name='cfbd_pipeline',
    destination='motherduck',
    dataset_name='cfbd',
    progress='enlighten'
)

load_info = pipeline.run(cfbd_source())
print(load_info)