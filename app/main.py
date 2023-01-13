#!/usr/bin/env python3
import argparse
import json
import logging
import sys
from typing import Union

import math
import requests
from requests import HTTPError

log = logging.getLogger(__name__)


def transform_all_docs_from_index_to_index(
        elasticsearch_url: str,
        index_1: str,
        index_2: str,
        size: int,
        size_of_part: int = 10000,
) -> None:

    counter = 0
    for part in range(math.ceil(size / size_of_part)):
        resp = requests.get(
            f"{elasticsearch_url}/{index_1}/_search?pretty",
            headers={"Content-type": "application/json"},
            data=json.dumps(
                {
                    "query": {
                        "range": {
                            "document_number": {
                                "gte": part * size_of_part,
                                "lt": (1 + part) * size_of_part,
                            }
                        }
                    },
                    "size": size_of_part,
                }
            ),
        )
        log.info(resp)
        create_docs_for_index(
            elasticsearch_url=elasticsearch_url,
            info_for_docs=resp.json()["hits"]["hits"],
            index=index_2,
        )
        counter += resp.json()['hits']['total']['value']
        log.info(resp.json()["hits"]["hits"])

    log.info(counter)
    assert counter == size
    log.log(1,"documents generated.")


def create_docs_for_index(
    elasticsearch_url: str,
    info_for_docs: list,
    index: str,
) -> Union[None, dict]:
    try:
        for doc in info_for_docs:
            calculated = give_length_of_dict(
                doc=doc,
                total_chars=0
            )
            info_for_new_doc = doc["_source"]
            info_for_new_doc.update({"calculated": calculated})
            resp_to_index_2 = requests.post(
                f"{elasticsearch_url}/{index}/_doc", json=info_for_new_doc
            )


            resp_to_index_2.raise_for_status()
    except HTTPError:
        return {"result": "Bad Request for url. Check your indexes and url"}

def give_length_of_dict(
    doc: dict,
    total_chars: int,
) -> int:
    for key, value in doc.items():

        try:
            assert type(value) is dict
            total_chars += give_length_of_dict(
                doc=value,
                total_chars=0
            ) + len(str(key))
            continue

        except AssertionError:
            total_chars += len(str(key)) + len(str(value))

    return total_chars

def main(args=sys.argv[1:]):
    logging.basicConfig(handlers=[logging.StreamHandler()])
    log.setLevel(logging.INFO)
    ap = argparse.ArgumentParser(description="Transform documents from index_1 and write it into index_2")
    ap.add_argument('--elasticsearch-url', type=str)
    ap.add_argument('--index1', type=str)
    ap.add_argument('--index2', type=str)
    args = ap.parse_args(args)
    log.info("parameters: %s", args)

    index_1_num_of_docs = requests.get(
        f"{args.elasticsearch_url}/{args.index1}/_count")

    log.info(f"{index_1_num_of_docs.json()['count']} asd")

    transform_all_docs_from_index_to_index(
        elasticsearch_url=args.elasticsearch_url,
        index_1=args.index1,
        index_2=args.index2,
        size=index_1_num_of_docs.json()['count'],
    )


if __name__ == '__main__':
    main()

