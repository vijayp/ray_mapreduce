import ray
import time
from collections import defaultdict
from itertools import islice
import math

def chunk(it, size):
    it = iter(it)
    return iter(lambda: list(islice(it, size)), [])


@ray.remote
class Mapper(object):
    def __init__(self, map_function, num_reducers):
        self._map_function = map_function
        self._num_reducers = num_reducers
        self._shard_to_buffer = [ [] for _ in range(self._num_reducers) ]
        self._done = False

    def _shard_for_key(self, key):
        return hash(key) % self._num_reducers

    def map(self, data):
        values = self._map_function(data)
        for k, v in values:
            self._shard_to_buffer[self._shard_for_key(k)].append((k,v))

    def bulk_map(self, data_list):
        for datum in data_list:
            self.map(datum)
        self.set_done()

    def set_done(self):
        self._done = True

    def get_pending_data_for_reducer(self, reducer_no):
        assert reducer_no < self._num_reducers
        rval = self._shard_to_buffer[reducer_no]
        if rval:
            # TODO: this should probably be atomic in a better way 
            # but for now ray runs things in an event loop so this should be blocking.
            self._shard_to_buffer[reducer_no] = []
            return rval
        elif self._done:
            return None
        else:
            return []


@ray.remote
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

def MapReduceBulk(data_list, map_fcn, reduce_fcn, num_mappers, num_reducers):
    chunks = list(chunk(data_list, math.ceil(len(data_list) / num_mappers)))
    mappers = [Mapper.remote(map_fcn, num_reducers) for _ in range(num_mappers)]
    for i, mapper in enumerate(mappers):
        mapper.bulk_map.remote(chunks[i])
    
    reducers = [Reducer.remote(reduce_fcn, mappers, shard) for shard in range(num_reducers)]
    output = []
    for reducer in reducers:
        output.append(reducer.reduce.remote())
    rval = []
    for ro in output:
        rval += ray.get(ro)
    return rval








