import os

# mongo
MONGO_CONNECTION = 'mongodb://steemit:steemit@mongo1.steemdata.com:27017/SteemData'
MONGO_DATABASE = 'SteemData'
MONGO_ACCOUNTS_TABLE = 'Accounts'
MONGO_POSTS_TABLE = 'Posts'

# process positions
STEEMD_POSITION_FILE_PATH = os.path.join(os.path.dirname(__file__), './data/steemd_position.json')
MONGO_POSITION_FILE_PATH = os.path.join(os.path.dirname(__file__), './data/mongo_position.json')

# url data
STEEMD_URL_DATA_FILE_PATH = os.path.join(os.path.dirname(__file__), './data/steemd_url_list.csv')
STEEMD_URL_COLUMNS = ['block_num', 'block_id', 'transaction_id', 'url', 'host_name', 'domain', 'author', 'parent_author', 'parent_permlink', 'permlink']

MONGO_URL_DATA_FILE_PATH = os.path.join(os.path.dirname(__file__), './data/mongo_url_list.csv')
MONGO_URL_COLUMNS = ['id', 'net_votes', 'total_payout_value', 'url', 'host_name', 'domain', 'author', 'parent_author', 'parent_permlink', 'permlink']

# author data
AUTHOR_DATA_FILE_PATH = os.path.join(os.path.dirname(__file__), './data/authors.csv')
AUTHOR_COLUMNS = ['name', 'created', 'last_vote_time', 'last_post', 'reputation', 'total_steem', 'total_sbd', 'total_vests']