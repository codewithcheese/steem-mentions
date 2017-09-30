import os
import pandas
import pymongo
from constants import (
    MONGO_URL_DATA_FILE_PATH, AUTHOR_DATA_FILE_PATH, AUTHOR_COLUMNS,
    MONGO_CONNECTION, MONGO_ACCOUNTS_TABLE, MONGO_DATABASE)

# init mongo
client = pymongo.MongoClient(MONGO_CONNECTION)

db = client[MONGO_DATABASE]
accounts = db[MONGO_ACCOUNTS_TABLE]

# load urls
if not os.path.exists(MONGO_URL_DATA_FILE_PATH):
    raise Exception('Url data most exist before running this script.')

url_data = pandas.DataFrame.from_csv(MONGO_URL_DATA_FILE_PATH)

# load author data
if os.path.exists(AUTHOR_DATA_FILE_PATH):
    author_data = pandas.DataFrame.from_csv(AUTHOR_DATA_FILE_PATH)
else:
    author_data = pandas.DataFrame(columns=AUTHOR_COLUMNS)

# create unique authors table for faster lookups
know_author_list = author_data['name'].unique()
know_author_table = dict()
for author in know_author_list:
    know_author_table[author] = True

author_list = url_data['author'].unique()
# for each unknown author
for author in author_list:
    if author not in know_author_table:
        print("Processing %s" % author)
        # get account data
        account = accounts.find_one({'name': author})
        # update author data frame
        entry = {
            'name': account['name'],
            'created': account['created'],
            'last_vote_time': account['last_vote_time'],
            'last_post': account['last_post'],
            'reputation': account['reputation'],
            'total_steem': account['balances']['total']['STEEM'],
            'total_sbd': account['balances']['total']['SBD'],
            'total_vests': account['balances']['total']['VESTS']
        }
        author_data.loc[len(author_data)] = entry
        # save updated author data
        author_data.to_csv(AUTHOR_DATA_FILE_PATH, columns=AUTHOR_COLUMNS)
        know_author_table[author] = True

