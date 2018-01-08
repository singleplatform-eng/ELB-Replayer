#!/usr/bin/env python
import argparse
import sys

import dateutil.parser
import requests
import itertools
from urlparse import urlparse

from twisted.internet import reactor

SCRIPT_DESCRIPTION = 'ELB Log Replayer (ELR)'
TOTALS = {'successful': 0, 'failed': 0}

SPINNER = itertools.cycle(['-', '\\', '|', '/'])
PARSER = argparse.ArgumentParser(description=SCRIPT_DESCRIPTION)
PARSER.add_argument('logfile', help='the logfile to replay')
PARSER.add_argument(
    '--host',
    help='host to send requests',
    default='localhost',
)
PARSER.add_argument(
    '--replace-host',
    action='store_false',
    help='use host parameter for request Host header'
)
PARSER.add_argument(
    '--verbose',
    action='store_true',
    help='always print requests and responses',
)
PARSER.add_argument(
    '--dry-run',
    action='store_true',
    help='don\'t actually hit the `host`',
)
PARSER.add_argument(
    '--paced',
    action='store_true',
    help='play requests as quickly as possible, not as they were in the original file'
)
PARSER.add_argument(
    '--limit',
    action='store',
    help='replay only first n requests'
)
PARSER.add_argument(
    '--dummy',
    action='store_true',
    help='treat 404=>200 transitions as successes (for when spdj_gatekeeper_dummy_response is true)'
)

SCRIPT_ARGS = PARSER.parse_args()


def replay_request(url, host, orig_resp):
    if not SCRIPT_ARGS.verbose:
        sys.stdout.write('\b')
        sys.stdout.write(SPINNER.next())
        sys.stdout.flush()
    if SCRIPT_ARGS.dry_run:
        sys.stdout.write('{}\n'.format(url))
    else:
        session = requests.Session()
        req = requests.Request('GET', 'http://{}{}?{}'.format(SCRIPT_ARGS.host, url.path, url.query))
        prepped = req.prepare()
        if SCRIPT_ARGS.replace_host:
            prepped.headers['Host'] = host
        resp = session.send(prepped)
        if SCRIPT_ARGS.dummy and int(orig_resp) == 404 and int(resp.status_code) == 200:
            resp.status_code = 404
        if str(resp.status_code) == str(orig_resp):
            warning = ''
            TOTALS['successful'] += 1
        else:
            warning = 'WARNING'
            TOTALS['failed'] += 1
        if SCRIPT_ARGS.verbose or warning == 'WARNING':
            print '{}=>{} {} {}?{}'.format(orig_resp, resp.status_code, warning, url.path, url.query)


def main():
    starting = None
    if SCRIPT_ARGS.limit:
        countdown = int(SCRIPT_ARGS.limit)
    else:
        countdown = None
    for line in open(SCRIPT_ARGS.logfile):
        bits = line.split()
        if SCRIPT_ARGS.paced:
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
        if SCRIPT_ARGS.limit:
            countdown -= 1
        if SCRIPT_ARGS.paced:
            reactor.callLater(offset.total_seconds(), replay_request, url, orig_host, orig_resp)
        else:
            reactor.callInThread(replay_request, url, orig_host, orig_resp)

    if SCRIPT_ARGS.paced:
        reactor.callLater(offset.total_seconds() + 2, reactor.stop)
    else:
        reactor.callFromThread(reactor.stop)

    reactor.run()

    print TOTALS

if __name__ == "__main__":
    print SCRIPT_ARGS
    if SCRIPT_ARGS.dry_run:
        SCRIPT_ARGS.paced = False
    main()
