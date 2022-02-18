#!/usr/bin/env python3
# Copyright (c) 2022 Vijay Pandurangan 


''' This is an example of how to use the mapreduce code. '''

import argparse
import logging
import sys


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='kickstartbio')
    parser.add_argument('-p', '--program_name', required=True, type=str, help='name of the program')
    parser.add_argument('-a', '--author', required=True, type=str, help='name for the author')
    parser.add_argument('-o', '--out', required=True, type=str, help='output directory')
    parser.add_argument('-v', '--verbose', default=False, action="store_true",
                        help="print details")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO if not args.verbose else logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ]
    )

    logging.info('starting up!')
    kickstartbio_lib.process(args.program_name, args.author, args.out)
    sys.exit(0)

