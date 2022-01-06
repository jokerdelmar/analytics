import pandas as pd
import os
import json
from pydiscourse import DiscourseClient
from pprint import pprint
from dotenv import load_dotenv   # for python-dotenv method
load_dotenv()                    # for python-dotenv method


# Discourse URL info.
url = "https://forum.bankless.community"
disco_user = os.environ.get('disco_user')
disco_key = os.environ.get('disco_key')

print()


# Connection with Discourse
client = DiscourseClient(
        url,
        api_username=disco_user,
        api_key=disco_key)

# Pulling single user Information
user = client.user('paulapivat')

# Building Dataframe with normalized data
df = pd.json_normalize(user)
# print(df)

# df.to_csv (r'user_info.csv', index = False, header=True)


# Pulling Topics
topics = client.new_topics()
pprint(topics)
