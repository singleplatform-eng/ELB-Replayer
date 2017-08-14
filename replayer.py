#!/usr/bin/env python
import argparse
import sys

import dateutil.parser
import requests
from urlparse import urlparse

from twisted.internet import reactor

description = 'ELB Log Replayer (ELR)'

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

script_args = parser.parse_args()


def replay_request(url, host, orig_resp):
    if script_args.dry_run:
        sys.stdout.write('{}\n'.format(url))
    else:
        s = requests.Session()
        req = requests.Request('GET', url)
        prepped = req.prepare()
        if script_args.preserve_host:
            prepped.headers['Host'] = host
        resp = s.send(prepped)
        if str(resp.status_code) == str(orig_resp):
            warning = ''
        else:
            warning = 'WARNING'
        if script_args.verbose:
            print '{}=>{} {} {}'.format(orig_resp, resp.status_code, warning, url)


def main():
    starting = None
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
        request_path = 'http://{}{}?{}'.format(
            script_args.host,
            url.path,
            url.query
        )
        reactor.callLater(offset.total_seconds(), replay_request, request_path, orig_host, orig_resp)

    reactor.callLater(offset.total_seconds() + 4, reactor.stop)
    reactor.run()

if __name__ == "__main__":
    main()
