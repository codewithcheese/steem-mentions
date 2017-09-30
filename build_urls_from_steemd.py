import os
import re
import json
import csv
import pandas
import traceback
import tldextract
from steem import Steem
from constants import STEEMD_URL_COLUMNS, STEEMD_URL_DATA_FILE_PATH, STEEMD_POSITION_FILE_PATH


if os.path.exists(STEEMD_POSITION_FILE_PATH):
    with open(STEEMD_POSITION_FILE_PATH) as position_handle:
        block_positions = json.loads(position_handle.read())
else:
    block_positions = {
        'LATEST_BLOCK': None,
        'EARLIEST_BLOCK': None,
        'SIZE': 10
    }

print(block_positions)

if os.path.exists(STEEMD_URL_DATA_FILE_PATH):
    url_data = pandas.DataFrame.from_csv(STEEMD_URL_DATA_FILE_PATH)

else:
    url_data = pandas.DataFrame([], columns=STEEMD_URL_COLUMNS)

tld = tldextract.TLDExtract()

def process_blocks(blocks):
    global url_data
    for block in blocks:
        for t, transaction in enumerate(block['transactions']):
            for operation in transaction['operations']:
                if operation[0] == 'comment':
                    if operation[1]['body'].slice(0, 2) == '@@':
                        continue  # skip edits
                    urls = re.findall(r"(?P<url>https?://[^\s\]\)]+)", operation[1]['body'])
                    for url in urls:

                        # parse url for host
                        host_parts = tld(url)
                        host_name = '%s%s.%s' % (
                        host_parts.subdomain + '.' if host_parts.subdomain else '', host_parts.domain,
                        host_parts.suffix)
                        domain = '%s.%s' % (host_parts.domain, host_parts.suffix)

                        entry = dict(
                            block_num=block['block_num'],
                            block_id=block['block_id'],
                            transaction_id=block['transaction_ids'][t],
                            url=url,
                            host_name=host_name,
                            domain=domain,
                            author=operation[1]['author'],
                            parent_author=operation[1]['parent_author'],
                            parent_permlink=operation[1]['parent_permlink'],
                            permlink=operation[1]['permlink'],
                        )
                        url_data.loc[len(url_data)] = entry

s = Steem()

head_block_number = s.head_block_number
if not block_positions['LATEST_BLOCK']:
    block_positions['LATEST_BLOCK'] = head_block_number
if not block_positions['EARLIEST_BLOCK']:
    block_positions['EARLIEST_BLOCK'] = head_block_number

# process descending to start with. since was giving more interesting initial results.
# processing starting from block 1 results were less interesting

# process descending from earliest block to block 1
earliest_block = block_positions['EARLIEST_BLOCK']
for n in range(earliest_block, 0, block_positions['SIZE'] * -1):
    new_earliest_block = n - block_positions['SIZE'] if n - block_positions['SIZE'] >= 0 else 0
    # shift range up one to include n
    print("Processing #%s to #%s" % (new_earliest_block + 1, n))
    blocks = s.get_blocks_range(new_earliest_block + 1, n + 1)
    process_blocks(blocks)
    block_positions['EARLIEST_BLOCK'] = new_earliest_block
    url_data.to_csv(STEEMD_URL_DATA_FILE_PATH, columns=STEEMD_URL_COLUMNS)
    with open(STEEMD_POSITION_FILE_PATH, 'w') as position_handle:
        position_handle.write(json.dumps(block_positions))

if False:
    # process ascending from latest block to head
    latest_block = block_positions['LATEST_BLOCK']
    for n in range(latest_block, head_block_number, block_positions['SIZE']):
        new_lastest_block = n + block_positions['SIZE'] if n + block_positions['SIZE'] <= head_block_number else head_block_number
        blocks = s.get_blocks_range(n, new_lastest_block)
        process_blocks(blocks)
        block_positions['LATEST_BLOCK'] = new_lastest_block
        url_data.to_csv(EARLIEST_BLOCK, columns=STEEMD_URL_COLUMNS)
        with open(STEEMD_POSITION_FILE_PATH, 'w') as position_handle:
            position_handle.write(json.dumps(block_positions))





