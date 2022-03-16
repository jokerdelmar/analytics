# libraries for postgres connection
import os
import sqlalchemy as sa

# http connection
import requests

# pandas
import pandas as pd

# using python-dotenv to access environment variables
from dotenv import load_dotenv

load_dotenv()

# NOTE: need to use environment variables to separate password from this file
# db_string = 'postgresql://user:password@localhost:port/mydatabase'
db_string = os.environ.get('DB_STRING')
engine = sa.create_engine(db_string)
print("Postgres Connected")


# traverse through data in increments of 1000 and break when end of file is encountered
# append the dict with each increment
data = pd.DataFrame()

for i in range(0, 999999, 1000):
    response = requests.get(
        f"https://api.poap.xyz/paginated-events?limit=1000&offset={i}"
        f"&sort_field=start_date&sort_dir=desc&private_event=false")
    json_data = response.json()
    # append the dict
    data = pd.json_normalize(json_data, 'items').append(data)

    # break condition for EOF
    if not json_data['items']:
        break

# filter for bankless events
mask = data['fancy_id'].str.contains('bankless', case=False, na=False)
bankless_event_id = data[mask]['id']
# convert id values to csv for graphql
bankless_event_list = bankless_event_id.values.tolist()

# POAP Subgraph GraphQL query
query = f"""{{
  events(where: {{id_in: {bankless_event_list}}},orderDirection: asc, subgraphError: allow) {{
    id
    tokenCount
    transferCount
    created
  }}
}}"""


def run_query():
    """description:
    Make request to BANK subgraph api endpoint
    Use 'query' object as parameters for request
    arg: 'q' or none
    return: POAP data as json object
    raise: Exception if api connection unsuccessful
    """
    request = requests.post('https://api.thegraph.com/subgraphs/name/poap-xyz/poap'
                            '',
                            json={'query': query})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed. return code is {}.     {}'.format(
            request.status_code, query))


def extract_list_of_dict():
    """description:
    Extract list of dictionaries from json object
    Each dictionary is a single POAP event
    arg: none
    return: list of dictionaries
    """
    result = run_query()
    result_items = result.items()
    result_list = list(result_items)
    list_of_dict = result_list[0][1].get('events')
    return list_of_dict


def transform_df():
    """description:
    Convert list_of_dict into dataframe
    Rename and reorder columns
    Important: Increment index with 'max_id' from postgres connection above
    Reset dataframe index in preparation for loading back to database
    args: none
    return: properly formatted dataframe
    """
    df = pd.json_normalize(extract_list_of_dict())
    # converting epoch to timestamp
    df['created'] = pd.to_datetime(df['created'], unit='s')
    df2 = df.rename(columns={'id': 'event_id',
                             'tokenCount': 'token_count',
                             'transferCount': 'transfer_count',
                             'created': 'created_ts'}, inplace=False)

    list_of_col_names = ['event_id', 'token_count', 'transfer_count', 'created_ts']
    df2 = df2.filter(list_of_col_names)
    # IMPORTANT
    # df2.index += max_id
    df2 = df2.reset_index()
    df3 = df2.rename(columns={'index': 'id'}, inplace=False)
    return df3


def load_df():
    """description:
    load poap data into poap_events table in postgres
    args: none
    return: properly formatted dataframe
    """
    dataframe = transform_df()
    dataframe.to_sql('poap_events', con=db_string,
                     if_exists='replace', index=False)
    print("successful push, poap_events table is now updated.")


# uncomment to run load_df() which pushes up backup to database
load_df()



