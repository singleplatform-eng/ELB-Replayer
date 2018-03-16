#!/usr/bin/env python
import argparse
import sys

import dateutil.parser
import requests
import itertools
from urlparse import urlparse, parse_qs

#from multiprocessing import Pool
from multiprocessing.dummy import Pool

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
    '--limit',
    action='store',
    help='replay only first n requests'
)
PARSER.add_argument(
    '--dummy',
    action='store_true',
    help='treat [45]0x=>200 transitions as successes (for when spdj_gatekeeper_dummy_response is true)'
)
PARSER.add_argument(
    '--output',
    action='store',
    help='store information about failed requests in output file'
)
PARSER.add_argument(
    '--suppress_keys_from',
    action='store',
    help='if supplied, suppress requests which use client ids in file'
)

SCRIPT_ARGS = PARSER.parse_args()
keys_to_suppress = []


def replay_request(url, host, orig_resp):
    if SCRIPT_ARGS.dry_run:
        sys.stdout.write('{}\n'.format(url))
    else:
        try:
            session = requests.Session()
            req = requests.Request('GET', 'http://{}{}?{}'.format(SCRIPT_ARGS.host, url.path, url.query))
            prepped = req.prepare()
            if SCRIPT_ARGS.replace_host:
                prepped.headers['Host'] = host
            resp = session.send(prepped)
            if SCRIPT_ARGS.dummy and int(orig_resp) in [0,404,403,401,500,502,503,504] and int(resp.status_code) == 200:
                resp.status_code = orig_resp
            if SCRIPT_ARGS.dummy and int(orig_resp) == 403 and int(resp.status_code) == 401:
                resp.status_code = orig_resp
            if str(resp.status_code) == str(orig_resp):
                warning = ''
                TOTALS['successful'] += 1
            else:
                warning = 'WARNING'
                TOTALS['failed'] += 1
                if SCRIPT_ARGS.output:
                    with open(SCRIPT_ARGS.output, 'a') as output:
                        output.write('{}=>{} {} {}?{}\n'.format(orig_resp, resp.status_code, resp.reason, url.path, url.query))
            if SCRIPT_ARGS.verbose or warning == 'WARNING':
                print '{}=>{} {} {}?{}'.format(orig_resp, resp.status_code, warning, url.path, url.query)
        except:
            pass


def process_line(line):
    if not SCRIPT_ARGS.verbose:
        sys.stdout.write('\b')
        sys.stdout.write(SPINNER.next())
        sys.stdout.flush()
    bits = line.split()
    method = bits[11].lstrip('"')
    if method != 'GET':
        return
    url = bits[12]
    url = urlparse(url)
    if SCRIPT_ARGS.suppress_keys_from:
        client_id = parse_qs(url.query).get('client', None)
        if client_id[0] in keys_to_suppress:
            TOTALS['suppressed'] += 1
            return
    orig_host = url.netloc.split(':')[0]
    orig_resp = bits[8]
    replay_request(url, orig_host, orig_resp)


def main():
    pool = Pool(processes=32)
    starting = None
    if SCRIPT_ARGS.suppress_keys_from:
        with open(SCRIPT_ARGS.suppress_keys_from) as f:
            for line in f:
                keys_to_suppress.append(line.rstrip())
        TOTALS.update({'suppressed': 0})
    with open(SCRIPT_ARGS.logfile) as f:
        if SCRIPT_ARGS.limit:
            countdown = int(SCRIPT_ARGS.limit)
            for line in f:
                if countdown == 0:
                    break
                countdown -= 1
                process_line(line)
        else:
            try:
                pool.map(process_line, f)
            except:
                pass

    pool.close()
    pool.join()

    print TOTALS

if __name__ == "__main__":
    print SCRIPT_ARGS
    main()
