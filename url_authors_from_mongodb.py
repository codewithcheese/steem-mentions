import os
import pandas
import pymongo
import csv
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

url_file = open(MONGO_URL_DATA_FILE_PATH, 'r')
url_csv = csv.DictReader(url_file)

author_table = dict()

for row in url_csv:
    author_table[row['author']] = True

# list of all authors to know
author_list = [item[0] for item in author_table.items()]

print('%s url authors' % len(author_list))

known_author_table = dict()

# initialize writer
if not os.path.exists(AUTHOR_DATA_FILE_PATH):
    author_file = open(AUTHOR_DATA_FILE_PATH, 'w')
    author_writer = csv.DictWriter(author_file, fieldnames=AUTHOR_COLUMNS)
    author_writer.writeheader()
else:
    with open(AUTHOR_DATA_FILE_PATH, 'r') as author_file:
        author_reader = csv.DictReader(author_file)
        for row in author_reader:
            known_author_table[row['name']] = True

    author_file = open(AUTHOR_DATA_FILE_PATH, 'a')
    author_writer = csv.DictWriter(author_file, fieldnames=AUTHOR_COLUMNS)

# for each author
for author in author_list:
    # if unknown
    if author not in known_author_table:
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
        author_writer.writerow(entry)
        author_file.flush()
        known_author_table[author] = True
    else:
        print('%s known' % author)

