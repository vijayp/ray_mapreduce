# Copyright (c) 2022 Vijay Pandurangan

import ray
import time
from collections import defaultdict
from itertools import islice
import math
from smart_open import open as smart_open
import hashlib
import functools
''' 
This is a simple implementation of an easy-to-use mapreduce using the ray.io framework in python.

For this to work, you must declare two functions:

    map_fcn is a generator function, which accepts one piece of data as an argument, and yields any number of (k,v) tuples

    reduce_fcn is a function which, when given as an argument, a key and a list of values, returns a specific output.

You can then provide a list of data and the number of mappers and reducers to the MapReduceBulk() and it will compute
the result in a distributed fashion. If you are running this on a ray cluster, the computation will be automatically 
sent to multiple computers for processing.

TODO: bulk interface is not optimal, because it is blocking and not streaming
TODO: we should support using a filesystem because currently the computer on which
    you run the main program has to process all data; it would be faster to use split datafiles

An example can be found in the unittest. Here is another. For each number between 0 and 1000, 
this will compute the sum of the number's square root and its square:

import mapreduce
import ray

ray.init() # note, you can supply an address to target a different cluster.

def map_fcn(data):
    yield data, data**2
    yield data, data**0.5    

def reduce_fcn(k, valuelist):
    return (k, sum(valuelist))

data = [x for x in range(1000)]
print(mapreduce.MapReduceBulk(data, map_fcn, reduce_fcn, 50, 5))
'''

def chunk(it, size):
    it = iter(it)
    return iter(lambda: list(islice(it, size)), [])


@ray.remote(num_cpus=1)
class Mapper(object):
    def __init__(self, map_function, num_reducers):
        self._map_function = map_function
        self._num_reducers = num_reducers
        self._shard_to_buffer = [ [] for _ in range(self._num_reducers) ]
        self._done = False

        # this is a bit crazy because we can't use the decorator due to
        # cloudpickle ray issues.
        self._do_hash = functools.lru_cache(maxsize=16*(1<<20))(self._shard_for_key_internal)
    
    def _shard_for_key(self, key):
        return self._do_hash(key)

    # again, this cannot be directly wrapped by lru_cache.
    def _shard_for_key_internal(self, key):
        shards = self._num_reducers
        h = int(hashlib.sha256(key.encode('utf-8')).hexdigest(), base=16)
        return h % shards

        # return shard_for_key(key, self._num_reducers)

    def map(self, data):
        values = self._map_function(data)
        for k, v in values:
            self._shard_to_buffer[self._shard_for_key(k)].append((k,v))

    def bulk_map(self, data_list, done=False):
        # TODO This will currently cache in memory all intermediate results
        # until the reducer asks for its piece.
        # for best performance, don't send chunks that are too large!
        for datum in data_list:
            self.map(datum)

        if done:
            self.done()

    def map_file_contents(self, filename, done=False):
        return self.bulk_map(smart_open(filename, 'r'))


    def done(self):
        self._done = True

    def get_pending_data_for_reducer(self, reducer_no):
        assert reducer_no < self._num_reducers
        rval = self._shard_to_buffer[reducer_no]
        if rval:
            self._shard_to_buffer[reducer_no] = []
            return rval
        elif self._done:
            return None
        else:
            return []


@ray.remote(num_cpus=1)
class Reducer(object):
    def __init__(self, reduce_function, mappers, my_shard):
        self._reduce_function = reduce_function
        self._mappers = mappers
        self._my_shard = my_shard
        self._done = False

    def reduce(self):
        to_process = defaultdict(list)
        outputs = []
        while self._mappers:
            new_mappers = []
            for i, m in enumerate(self._mappers):
                values = m.get_pending_data_for_reducer.remote(self._my_shard)
                values = ray.get(values)
                if values is not None:
                    new_mappers.append(m)
                    for (k,v) in values:
                        to_process[k].append(v)
            self._mappers = new_mappers
            if new_mappers:
                time.sleep(1)
        for k, v_list in to_process.items():
            outputs.append(self._reduce_function(k, v_list))
        self._done = True
        return outputs



def MapReduceBulk(data_list, map_fcn, reduce_fcn, num_mappers, num_reducers, max_chunk_size=1000, dataset_size=None, distribute_work_fcn=None):
    def default_distribute_work_fcn(data_list, chunk_size, mappers):
        num_mappers = len(mappers)
        for i, data_chunk in enumerate(chunk(data_list, chunk_size)):
            mapper = mappers[i % num_mappers]
            mapper.bulk_map.remote(data_chunk)
    if distribute_work_fcn is None:
        distribute_work_fcn = default_distribute_work_fcn

    if dataset_size is None:
        dataset_size = len(data_list)
    chunk_size = min(max_chunk_size, math.ceil(dataset_size / num_mappers))
    mappers = [Mapper.remote(map_fcn, num_reducers) for _ in range(num_mappers)]
    reducers = [Reducer.remote(reduce_fcn, mappers, shard) for shard in range(num_reducers)]
    output = []
    for reducer in reducers:
        output.append(reducer.reduce.remote())
    distribute_work_fcn(data_list, chunk_size, mappers)

    for m in mappers:
        m.done.remote()

    rval = []
    for ro in output:
        rval += ray.get(ro)
    return rval


def MapReduceWithOneFileInput(filename, map_fcn, reduce_fcn, num_mappers, num_reducers, max_chunk_size=1000, ignore_first_line=False):
    with smart_open(filename, 'r') as fd:
        if ignore_first_line:
            next(fd)
        # we can't easily estimate the number of lines in the file, so set it to a big number
        dataset_size = 1<<30
        return MapReduceBulk(fd, map_fcn, reduce_fcn, num_mappers,num_reducers, max_chunk_size, dataset_size)


def MapReduceWithMultipleFiles(index_filename, map_fcn, reduce_fcn, num_mappers, num_reducers):
    def distribute_filenames_fcn(filename_list, chunk_size, mappers):
        num_mappers = len(mappers)
        for i, filename in enumerate(filename_list):
            mapper = mappers[i % num_mappers]
            mapper.map_file_contents.remote(filename)


    with smart_open(index_filename, 'r') as index_fn:
        filenames = [f.strip() for f in index_fn]
        # distribute the filenames.
        return MapReduceBulk(filenames,  map_fcn, reduce_fcn, num_mappers,num_reducers, distribute_work_fcn=distribute_filenames_fcn)






