import ray
import time
from collections import defaultdict
from itertools import islice
from ray.util.queue import Queue

def chunk(it, size):
    it = iter(it)
    return iter(lambda: list(islice(it, size)), [])


@ray.remote
class Mapper(object):
    def __init__(self, map_function, reducer_queues):
        self._map_function = map_function
        self._reducer_queues = reducer_queues
        self._num_reducers = len(reducer_queues)
        self._shard_to_buffer = [ [] for _ in range(self._num_reducers) ]

    def _shard_for_key(self, key):
        return hash(key) % self._num_reducers

    def map(self, data):
        values = self._map_function(data)
        for k, v in values:
            self._shard_to_buffer[self._shard_for_key(k)].append((k,v))

    def bulk_map(self, data_list):
        print('mapper data list' , data_list)
        for datum in data_list:
            print(datum)
            self.map(datum)
        # todo send these in smaller chunks?
        for shard, kvlist in enumerate(self._shard_to_buffer):
            print('->',shard, kvlist)
            self._reducer_queues[shard].put(kvlist)
        for q in self._reducer_queues:
            q.put(None)

@ray.remote
class Reducer(object):
    def __init__(self, reduce_function, in_queue, num_mappers):
        self._reduce_function = reduce_function
        self._in_queue = in_queue
        self._num_mappers_left = num_mappers

    def reduce(self):
        to_process = defaultdict(list)
        while self._num_mappers_left:
            values = self._in_queue.get(block=True)
            print(values, self._num_mappers_left)
            if values is None:
                self._num_mappers_left -= 1
                continue
            for (k,v) in values:
                to_process[k].append(v)
            outputs = []
            print ('reduce done with %s' % ( str(to_process.items())))
            for k, v_list in to_process.items():
                outputs.append(self._reduce_function(k, v_list))
            print ('reduce done with %s' % (str(outputs)))
            self._done = True
        return outputs

def MapReduceBulk(data_list, map_fcn, reduce_fcn, num_mappers, num_reducers, max_queue_size=1<<10):
    print('dl', data_list)
    chunks = list(chunk(data_list, num_mappers))

    print ('chunks: ', chunks)
    queues = [Queue(maxsize=max_queue_size) for _ in range(num_reducers)]

    mappers = [Mapper.remote(map_fcn, queues) for _ in range(num_mappers)]
    for i, mapper in enumerate(mappers):
        mapper.bulk_map.remote(chunks[i])
    
    reducers = [Reducer.remote(reduce_fcn, queues[i], num_mappers) for i in range(num_reducers)]
    output = []
    for reducer in reducers:
        output.append(reducer.reduce.remote())
    rval = []
    time.sleep(1)
    print('FECTHING!!')
    for ro in output:
        rval += ray.get(ro)
    return rval








