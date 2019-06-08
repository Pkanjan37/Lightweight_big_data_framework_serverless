# import numpy as np
# from sympy import *
# def my_map_function(x):
#     return my_map2(x) + 7
# def my_map2(x):
#     return x*3
# def printNP(x):
#     return np.random.randint(x, 10, size=[2,2])
# def printEQ(x):
#     x, y, z, t = symbols('x y z t')
#     a = Rational(3,2)*pi + exp(I*x) / (x**2 + y)
#     return a
def predefine_put_data(self,call_id,timeStmp,func):
        path = func+"-"+str(timeStmp)+"/"+call_id
        return path

import time
import boto3
import json
import jsonpickle
call_id='00001'
a = predefine_put_data('',call_id,time.time(),'map_map2')
print(a)
plusfile = a+"/input2.json"
bucket= 'lightweight-1'
s3 = boto3.client('s3')
input=5
data = {"data":input,"isUrl":False,"call_id":call_id}
data = jsonpickle.encode(data)
print(data)
# data = json.dumps(data)
s3.put_object(Bucket=bucket, Key=plusfile, Body=data)



