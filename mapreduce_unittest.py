from cgi import test
from stringprep import map_table_b2
import unittest
import mapreduce 
import ray
import time

def map_fcn(data):
    yield data % 9, data**2

def reduce_fcn(k, valuelist):
    return (k, max(valuelist))

class MapReduceTest(unittest.TestCase):
    def setUp(self):
        ray.init()
        self._testdata = range(1000)


        self._output = [(0, 998001),
                        (1, 982081),
                        (2, 984064),
                        (3, 986049),
                        (4, 988036),
                        (5, 990025),
                        (6, 992016),
                        (7, 994009),
                        (8, 996004)]
        pass

    def tearDown(self):
        ray.shutdown()
        # tear stuff down here
        pass

    def testMapReduceDirect(self):
        REDUCERS = 10
        mappers = [mapreduce.Mapper.remote(map_fcn, REDUCERS)]
        for mapper in mappers:
            mapper.bulk_map.remote(self._testdata)
        
        reducers = [mapreduce.Reducer.remote(reduce_fcn, mappers, shard) for shard in range(REDUCERS)]
        output = []
        for reducer in reducers:
            output.append(reducer.reduce.remote())
        outputs = []
        for reducer_out in output:
            for k,v in ray.get(reducer_out):
                outputs.append((k,v))

        self.assertEqual(outputs, self._output)
        

    def testBulkMapReduce(self):
        num_mappers = 3
        num_reducers = 4
        output = mapreduce.MapReduceBulk(self._testdata, map_fcn, reduce_fcn, num_mappers, num_reducers)
        #self.assertEqual(output, self._output)
        
        


if __name__ == '__main__':
    unittest.main()