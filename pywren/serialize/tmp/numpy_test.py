import json
import boto3
import jsonpickle
import time
import signal
# from sympy import *
import numpy
import time
def my_map_function3(x):
    return my_map3(x) + 7
def my_map3(x):
    return x-20
def summer_sol(x):
    return sum(x)+5
def numpy_test(x):
    return numpy.arange(15).reshape(3, 5)
def expire(x):
    time.sleep(30)
# def printEQ(x):
def lambda_handler(event, context):
   try:
       with Timeout(15):
           s3 = boto3.client('s3')
           bucket_in= 'xifer-pywren-118'
           plusfile = event['input']
           r = s3.get_object(Bucket=bucket_in, Key=plusfile)
           input_data = r['Body'].read().decode()
           received_data = json.loads(input_data)
           inputData = received_data['data']
           compute = numpy_test(inputData)
           bucket_out= 'output-bucky'
           compute = jsonpickle.encode(compute)
           output_data = {'output':compute}
           output_data = json.dumps(output_data)
           s3.put_object(Bucket=bucket_out, Key=plusfile, Body=output_data)
           return {
               'statusCode': 200,
           }
   except Timeout.Timeout:
       print ('Timeout')
class Timeout():
   class Timeout(Exception):
       pass
   def __init__(self, sec):
       self.sec = sec
   def __enter__(self):
       signal.signal(signal.SIGALRM, self.raise_timeout)
       signal.alarm(self.sec)
   def __exit__(self, *args):
       signal.alarm(0)
   def raise_timeout(self, *args):
       raise  TimeoutError('Execution time exceed the limit')
