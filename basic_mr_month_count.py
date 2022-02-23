#!/usr/bin/env python3
from pickle import TRUE
import sys
from collections import defaultdict
import mapreduce
import ray
from ray.autoscaler.sdk import request_resources
import time

def mymap(line):
    DATE_COL = 3
    VARIANT_COL = 13
    VARIANT_NAME_WORDNO = 1
    try:
        col_list = line.split('\t')
        variant_str = col_list[VARIANT_COL]
        variant = variant_str.split(' ')[VARIANT_NAME_WORDNO]
    except IndexError:
        yield ('no_variant', 1)
        return
    try:
        month = col_list[DATE_COL][:7]
    except IndexError:
        yield ('date_error', 1)
        return
    
    yield ((variant + '\t' + month), 1)

def myreduce(k, value_list):
    return '%s, %d' % (k, sum(value_list))

if __name__ == '__main__':
    num_reducers = 20
    num_mappers = 20
    ray.init(
        address='auto'
    )
    request_resources(num_cpus=(num_mappers + num_reducers))
    time.sleep(10)
    rval = mapreduce.MapReduceWithMultipleFiles(
        sys.argv[1],
        mymap,
        myreduce,
        num_reducers,
        num_mappers)
    print('\n'.join(rval))


