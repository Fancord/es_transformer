#!/usr/bin/env python3

import sys
import logging
import argparse
import time


import requests

log = logging.getLogger(__name__)

def generate_docs(elasticsearch_url, index, size):
    log.info("cleaning old index...")
    out = requests.delete(f"{elasticsearch_url}/{index}")
    log.info(out)
    log.info(out.json())
    log.info("OK")

    log.info("generating documents...")
    for _ in range(size):
        resp = requests.post(f"{elasticsearch_url}/{index}/_doc", json=dict(
            document_number=_,
            calculated=20
            ))
        resp.raise_for_status()
    time.sleep(2)
    log.info("documents generated.")

def main(args=sys.argv[1:]):
    logging.basicConfig(handlers=[logging.StreamHandler()])
    log.setLevel(logging.INFO)
    ap = argparse.ArgumentParser(description="Recreates index (delete and create) and push data into it")
    ap.add_argument('--elasticsearch-url', type=str)
    ap.add_argument('--index', type=str)
    ap.add_argument('--size', type=int)
    args = ap.parse_args(args)
    log.info("parameters: %s", args)

    generate_docs(args.elasticsearch_url, args.index, args.size)




if __name__ == '__main__':
    main()

