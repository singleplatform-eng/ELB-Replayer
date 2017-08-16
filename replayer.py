#!/usr/bin/env python
import argparse
import sys

import dateutil.parser
import requests
import itertools
from urlparse import urlparse

from twisted.internet import reactor

description = 'ELB Log Replayer (ELR)'
totals = {'successful': 0, 'failed': 0}

spinner = itertools.cycle(['-', '\\', '|', '/'])
parser = argparse.ArgumentParser(description=description)
parser.add_argument('logfile', help='the logfile to replay')
parser.add_argument(
    '--host',
    help='host to send requests',
    default='localhost',
)
parser.add_argument(
    '--replace-host',
    action='store_false',
    help='use host parameter for request Host header'
)
parser.add_argument(
    '--verbose',
    action='store_true',
    help='always print requests and responses',
)
parser.add_argument(
    '--dry-run',
    action='store_true',
    help='don\'t actually hit the `host`',
)
parser.add_argument(
    '--paced',
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
    if not script_args.verbose:
        sys.stdout.write('\b')
        sys.stdout.write(spinner.next())
        sys.stdout.flush()
    if script_args.dry_run:
        sys.stdout.write('{}\n'.format(url))
    else:
        s = requests.Session()
        req = requests.Request('GET', 'http://{}{}?{}'.format(script_args.host, url.path, url.query))
        prepped = req.prepare()
        if script_args.replace_host:
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
        countdown = int(script_args.limit)
    else:
        countdown = None
    for line in open(script_args.logfile):
        bits = line.split()
        if script_args.paced:
            timestamp = dateutil.parser.parse(bits[0])
            if not starting:
                starting = timestamp
            offset = timestamp - starting
            if offset.total_seconds() < 0:
                # ignore past requests
                continue
        method = bits[11].lstrip('"')
        if method != 'GET':
            continue
        url = urlparse(bits[12])
        orig_host = url.netloc.split(':')[0]
        orig_resp = bits[8]
        if countdown == 0:
            break
        if script_args.limit:
            countdown -= 1
        if script_args.paced:
            reactor.callLater(offset.total_seconds(), replay_request, url, orig_host, orig_resp)
        else:
            reactor.callInThread(replay_request, url, orig_host, orig_resp)

    if script_args.paced:
        reactor.callLater(offset.total_seconds() + 2, reactor.stop)
    else:
        reactor.callFromThread(reactor.stop)

    reactor.run()

    print totals

if __name__ == "__main__":
    print script_args
    if script_args.dry_run:
        script_args.paced = False
    main()
