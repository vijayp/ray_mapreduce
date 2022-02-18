import unittest
import mapreduce 
import ray
import time

num_mappers = 3
num_reducers = 4
ray.init()
testdata = [x for x in range(1000)]

def map_fcn(data):
    yield data % 9, data**2

def reduce_fcn(k, valuelist):
    return (k, max(valuelist))
output = mapreduce.MapReduceBulk(testdata, map_fcn, reduce_fcn, num_mappers, num_reducers)
print(output)
