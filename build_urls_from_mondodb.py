import os
import re
import json
import csv
import pandas
import traceback
import pymongo
import tldextract
from steem import Steem
from constants import (MONGO_URL_DATA_FILE_PATH, MONGO_URL_COLUMNS, MONGO_POSITION_FILE_PATH, MONGO_CONNECTION,
                        MONGO_DATABASE, MONGO_POSTS_TABLE)


if os.path.exists(MONGO_POSITION_FILE_PATH):
    with open(MONGO_POSITION_FILE_PATH) as position_handle:
        positions = json.loads(position_handle.read())
else:
    positions = {
        'FROM': 0,
        'SIZE': 1000
    }

if os.path.exists(MONGO_URL_DATA_FILE_PATH):
    url_data = pandas.DataFrame.from_csv(MONGO_URL_DATA_FILE_PATH)

else:
    url_data = pandas.DataFrame([], columns=MONGO_URL_COLUMNS)

tld = tldextract.TLDExtract()

def process_post(post):
    global url_data
    url_table = dict()
    if not post['body'][0: 2] == '@@':
        urls = re.findall(r"(?P<url>https?://[^\s\]\)]+)", post['body'])
        for url in urls:
            if url not in url_table: # only unique urls per post
                url_table[url] = True

                # parse url for host
                host_parts = tld(url)
                host_name = '%s%s.%s' % (host_parts.subdomain + '.' if host_parts.subdomain else '', host_parts.domain, host_parts.suffix)
                domain = '%s.%s' % (host_parts.domain, host_parts.suffix)
                entry = dict(
                    id=post['id'],
                    url=url,
                    host_name=host_name,
                    domain=domain,
                    author=post['author'],
                    parent_author=post['parent_author'],
                    parent_permlink=post['parent_permlink'],
                    permlink=post['permlink'],
                )
                url_data.loc[len(url_data)] = entry

client = pymongo.MongoClient(MONGO_CONNECTION)

db = client[MONGO_DATABASE]
posts = db[MONGO_POSTS_TABLE]

size = positions['SIZE']
next = positions['FROM']
while True:
    #
    print("Processing from %s to %s" % (next, next+size))
    for post in posts.find()[next: next+size]:
        process_post(post)

    next += size
    positions['FROM'] = next

    url_data.to_csv(MONGO_URL_DATA_FILE_PATH, columns=MONGO_URL_COLUMNS)
    with open(MONGO_POSITION_FILE_PATH, 'w') as position_handle:
        position_handle.write(json.dumps(positions))








