#!/usr/bin/env python
import argparse
import sys

import dateutil.parser
import requests
from urlparse import urlparse

from twisted.internet import reactor

description = 'ELB Log Replayer (ELR)'
totals = {'successful': 0, 'failed': 0}

parser = argparse.ArgumentParser(description=description)
parser.add_argument('logfile', help='the logfile to replay')
parser.add_argument(
    '--host',
    help='host to send requests',
    default='localhost',
)
parser.add_argument(
    '--preserve-host',
    action='store_true',
    help='set host header from original file.',
)
parser.add_argument(
    '--verbose',
    action='store_true',
    help='print requests and responses',
)
parser.add_argument(
    '--dry-run',
    action='store_true',
    help='don\'t actually hit the `host`',
)
parser.add_argument(
    '--asap',
    action='store_true',
    help='play requests as quickly as possible, not as they were in the original file'
)
parser.add_argument(
    '--limit',
    action='store',
    help='replay only first n requests'
)

script_args = parser.parse_args()


def replay_request(url, host, orig_resp):
    if script_args.dry_run:
        sys.stdout.write('{}\n'.format(url))
    else:
        s = requests.Session()
        req = requests.Request('GET', 'http://{}{}?{}'.format(script_args.host, url.path, url.query))
        prepped = req.prepare()
        if script_args.preserve_host:
            prepped.headers['Host'] = host
        resp = s.send(prepped)
        if str(resp.status_code) == str(orig_resp):
            warning = ''
            totals['successful'] += 1
        else:
            warning = 'WARNING'
            totals['failed'] += 1
        if script_args.verbose or warning == 'WARNING':
            print '{}=>{} {} {}?{}'.format(orig_resp, resp.status_code, warning, url.path, url.query)


def main():
    starting = None
    if script_args.limit:
        counter = int(script_args.limit)
    else:
        counter = None
    for line in open(script_args.logfile):
        bits = line.split()
        timestamp = dateutil.parser.parse(bits[0])
        if not starting:
            starting = timestamp
        offset = timestamp - starting
        if offset.total_seconds() < 0:
            # ignore past requests
            continue
        method = bits[11].lstrip('"')
        url = urlparse(bits[12])
        if method != 'GET':
            continue
        orig_host = url.netloc.split(':')[0]
        orig_resp = bits[8]
        if counter == 0:
            break
        if script_args.limit:
            counter -= 1
        if script_args.asap or script_args.dry_run:
            replay_request(url, orig_host, orig_resp)
        else:
            reactor.callLater(offset.total_seconds(), replay_request, url, orig_host, orig_resp)

    if not script_args.asap:
        reactor.callLater(offset.total_seconds() + 4, reactor.stop)
        reactor.run()

    print totals

if __name__ == "__main__":
    main()
