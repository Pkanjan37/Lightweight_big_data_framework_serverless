import json
import boto3
# from sympy import *
import numpy
def my_map_function3(x):
    return my_map3(x) + 7
def my_map3(x):
    return x-20
def summer_sol(x):
    return sum(x)+5
def numpy_test(x):
    return numpy.arange(x**2, dtype=numpy.float64).reshape(x, x)
# def printEQ(x):
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_in= 'xifer-pywren-118'
    plusfile = event['input']
    r = s3.get_object(Bucket=bucket_in, Key=plusfile)
    input_data = r['Body'].read().decode()
    received_data = json.loads(input_data)
    inputData = received_data['data']
    compute = numpy_test(inputData)
    bucket_out= 'output-bucky'
    output_data = {'output':compute}
    output_data = json.dumps(output_data)
    s3.put_object(Bucket=bucket_out, Key=plusfile, Body=output_data)
    return {
        'statusCode': 200,
        'body': json.dumps(compute)
    }
