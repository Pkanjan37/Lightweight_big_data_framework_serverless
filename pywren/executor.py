#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import
from __future__ import print_function

import logging
import random
import time
from multiprocessing.pool import ThreadPool
from six.moves import cPickle as pickle
import json

import boto3

import pywren.runtime as runtime
import pywren.storage as storage
import pywren.version as version
import pywren.wrenconfig as wrenconfig
import pywren.wrenutil as wrenutil

from pywren.future import ResponseFuture, JobState
from pywren.state import ResponseStateFuture
from pywren.serialize import serialize, create_mod_data, buddleInitor
from pywren.jobrunner import stepFunctionbuilder
from pywren.storage import storage_utils
from pywren.storage.storage_utils import create_func_key
from pywren.wait import wait, ALL_COMPLETED
import jsonpickle
import sys
logger = logging.getLogger(__name__)


"""
Theoretically will allow for cross-AZ invocations
"""
class Executor(object):

    def __init__(self, config, job_max_runtime):
        # self.invoker = invoker
        self.job_max_runtime = job_max_runtime

        self.config = config
        self.storage_config = wrenconfig.extract_storage_config(self.config)
        self.storage = storage.Storage(self.storage_config)
        # self.runtime_meta_info = runtime.get_runtime_info(config['runtime'])
        self.stateMachine_arn=""
        self.input_list=[]
        self.output_path=[]
        self.storage_path = storage_utils.get_storage_path(self.storage_config)

        # if 'preinstalls' in self.runtime_meta_info:
        #     logger.info("using serializer with meta-supplied preinstalls")
        #     self.serializer = serialize.SerializeIndependent(self.runtime_meta_info['preinstalls'])
        # else:
        #     self.serializer = serialize.SerializeIndependent()
        self.serializer = serialize.SerializeIndependent()

        self.map_item_limit = None
        if 'scheduler' in self.config:
            if 'map_item_limit' in config['scheduler']:
                self.map_item_limit = config['scheduler']['map_item_limit']

    def put_data(self, data_key, data_str,
                 callset_id, call_id):

        self.storage.put_data(data_key, data_str)
        logger.info("call_async {} {} data upload complete {}".format(callset_id, call_id,
                                                                      data_key))
        print("Put data <<<<< "+data_key)

    def invoke_with_keys(self, func_key, data_key, output_key,
                         status_key, cancel_key,
                         callset_id, call_id, extra_env,
                         extra_meta, data_byte_range, use_cached_runtime,
                         host_job_meta, job_max_runtime,
                         overwrite_invoke_args=None):

        # Pick a runtime url if we have shards.
        # If not the handler will construct it
        runtime_url = ""
        if ('urls' in self.runtime_meta_info and
                isinstance(self.runtime_meta_info['urls'], list) and
                len(self.runtime_meta_info['urls']) >= 1):
            num_shards = len(self.runtime_meta_info['urls'])
            logger.debug("Runtime is sharded, choosing from {} copies.".format(num_shards))
            random.seed()
            runtime_url = random.choice(self.runtime_meta_info['urls'])

        arg_dict = {
            'storage_config' : self.storage.get_storage_config(),
            'func_key' : func_key,
            'data_key' : data_key,
            'output_key' : output_key,
            'status_key' : status_key,
            'cancel_key' : cancel_key,
            'callset_id': callset_id,
            'job_max_runtime' : job_max_runtime,
            'data_byte_range' : data_byte_range,
            'call_id' : call_id,
            'use_cached_runtime' : use_cached_runtime,
            'runtime' : self.config['runtime'],
            'pywren_version' : version.__version__,
            'runtime_url' : runtime_url}

        if extra_env is not None:
            logger.debug("Extra environment vars {}".format(extra_env))
            arg_dict['extra_env'] = extra_env

        if extra_meta is not None:
            # sanity
            for k, v in extra_meta.items():
                if k in arg_dict:
                    raise ValueError("Key {} already in dict".format(k))
                arg_dict[k] = v

        host_submit_time = time.time()
        arg_dict['host_submit_time'] = host_submit_time

        logger.info("call_async {} {} lambda invoke ".format(callset_id, call_id))
        lambda_invoke_time_start = time.time()

        # overwrite explicit args, mostly used for testing via injection
        if overwrite_invoke_args is not None:
            arg_dict.update(overwrite_invoke_args)

        # do the invocation
        # self.invoker.invoke(arg_dict)

        host_job_meta['lambda_invoke_timestamp'] = lambda_invoke_time_start
        host_job_meta['lambda_invoke_time'] = time.time() - lambda_invoke_time_start


        # host_job_meta.update(self.invoker.config())

        logger.info("call_async {} {} lambda invoke complete".format(callset_id, call_id))


        host_job_meta.update(arg_dict)

        storage_path = storage_utils.get_storage_path(self.storage_config)
        # self.storage_path = storage_path
        print('Invoke storage conf <<< '.join(['{0}{1}'.format(k, v) for k,v in self.storage_config.items()]))
        print("Invoke storage path<<<<<".join(map(str, storage_path)))
        print('Invokemetadata <<< '.join(['{0}{1}'.format(k, v) for k,v in host_job_meta.items()]))
        print("Invoker metadata<<<<< "+call_id)
        print("Invoker metadata<<<<< "+callset_id)
        fut = ResponseFuture(call_id, callset_id, host_job_meta, storage_path)

        fut._set_state(JobState.invoked)

        return fut

    def call_async(self, func, data, extra_env=None,
                   extra_meta=None):
        return self.map(func, [data], extra_env, extra_meta)

    @staticmethod
    def agg_data(data_strs):
        ranges = []
        pos = 0
        for datum in data_strs:
            l = len(datum)
            ranges.append((pos, pos + l -1))
            pos += l
        return b"".join(data_strs), ranges

    def map(self, func, iterdata, extra_env=None, extra_meta=None,
            invoke_pool_threads=64, data_all_as_one=True,
            use_cached_runtime=True, overwrite_invoke_args=None,
            exclude_modules=None,instance_specify=None,s3_file_url=False):
        """
        :param func: the function to map over the data
        :param iterdata: An iterable of input data
        :param extra_env: Additional environment variables for lambda environment. Default None.
        :param extra_meta: Additional metadata to pass to lambda. Default None.
        :param invoke_pool_threads: Number of threads to use to invoke.
        :param data_all_as_one: upload the data as a single object. Default True
        :param use_cached_runtime: Use cached runtime whenever possible. Default true
        :param overwrite_invoke_args: Overwrite other args. Mainly used for testing.
        :param exclude_modules: Explicitly keep these modules from pickled dependencies.
        :return: A list with size `len(iterdata)` of futures for each job
        :rtype:  list of futures.

        Usage
          >>> futures = pwex.map(foo, data_list)
        """
        print("execute0 <<<<<<<")
        import os
        import inspect
        # print(os.path.realpath(__file__))
        # print (__file__) 
        # print (inspect.getfile(inspect.currentframe()))
        # print (os.path.basename(__file__))
        # print (os.path.abspath(inspect.stack()[0][1]))
        # print (inspect.stack()[1][1])
        print(s3_file_url)
        # raise Exception("eieieie")
        print (os.path.abspath(inspect.stack()[-1][1]))
        print("execute1 <<<<<<<<")
        print(iterdata)
        print(func)
        # import inspect
        # lines = inspect.getsource(func)
        # print("execute1.1 <<<<<<<<")
        # print(lines)
        # print("execute1.2 <<<<<<<<")
        # print(func.__name__)
        # print("execute1.2 <<<<<<<<")
        
        print("execute1 <<<<<<<<")
        data = list(iterdata)
        print(data)
        # raise Exception("Eieieieie")
        self.input_list = data
        # print("execute2 <<<<<<<<")
        # print(data)
        # print("execute2 <<<<<<<<")
        if not data:
            return []
        if instance_specify == None:
            instance_input = "small"
            for i in data:
                estimate_input_size = sys.getsizeof(i)
                if estimate_input_size <= 45000000:
                    instance_input = "small" 
                elif estimate_input_size < 500000000 and estimate_input_size>45000000 and instance_input=="small":
                    instance_input = "medium"
                elif estimate_input_size< 1000000000 and estimate_input_size> 500000000 and (instance_input=="small" or instance_input == "medium"):
                    instance_input = "large"
                else: raise Exception(" The size of input for each worker is exceeded maximum ")
            print(instance_input)
        else : instance_input = instance_specify
            # raise Exception("eiei")

        if self.map_item_limit is not None and len(data) > self.map_item_limit:
            raise ValueError("len(data) ={}, exceeding map item limit of {}"\
                             "consider mapping over a smaller"\
                             "number of items".format(len(data),
                                                      self.map_item_limit))

        # host_job_meta = {}
        timeStampId=time.time()

        # pool = ThreadPool(invoke_pool_threads)
        callset_id = wrenutil.create_callset_id()

        ### pickle func and all data (to capture module dependencies
        func_and_data_ser, mod_paths = self.serializer([func] + data)

        # func_str = func_and_data_ser[0]
        # print("execute3 <<<<<<<<")
        # print(func_str)
        # print("execute3 <<<<<<<<")
        # data_strs = func_and_data_ser[1:]
        # print("execute4 <<<<<<<<")
        # print(data_strs)
        # print("execute4 <<<<<<<<")
        # data_size_bytes = sum(len(x) for x in data_strs)
        # agg_data_key = None
        # host_job_meta['agg_data'] = False
        # host_job_meta['data_size_bytes'] = data_size_bytes

        # if data_size_bytes < wrenconfig.MAX_AGG_DATA_SIZE and data_all_as_one:
        #     agg_data_key = storage_utils.create_agg_data_key(self.storage.prefix, callset_id)
        #     agg_data_bytes, agg_data_ranges = self.agg_data(data_strs)
        #     # put data here
        #     print("<<<<< put data")
        #     print(agg_data_key)
        #     print(agg_data_bytes)
        #     print(agg_data_ranges)
        #     print("<<<<< put data")
        #     agg_upload_time = time.time()
        #     self.storage.put_data(agg_data_key, agg_data_bytes)
        #     host_job_meta['agg_data'] = True
        #     host_job_meta['data_upload_time'] = time.time() - agg_upload_time
        #     host_job_meta['data_upload_timestamp'] = time.time()
        # else:
        #     # FIXME add warning that you wanted data all as one but
        #     # it exceeded max data size
        #     pass

        if exclude_modules:
            for module in exclude_modules:
                for mod_path in list(mod_paths):
                    if module in mod_path and mod_path in mod_paths:
                        mod_paths.remove(mod_path)

        module_data = create_mod_data(mod_paths)
        # print("execute5 <<<<<<<<")
        print(mod_paths)
        buddleInitor.zipper(mod_paths,os.path.abspath(inspect.stack()[-1][1]),func.__name__,self.config,self.storage,func)
        # print("execute5 <<<<<<<<")
        ### Create func and upload
        # func_module_str = pickle.dumps({'func' : func_str,
                                        # 'module_data' : module_data}, -1)
        # host_job_meta['func_module_str_len'] = len(func_module_str)

        # func_upload_time = time.time()
        # func_key = create_func_key(self.storage.prefix, callset_id)
        # self.storage.put_func(func_key, func_module_str)
        # host_job_meta['func_upload_time'] = time.time() - func_upload_time
        # host_job_meta['func_upload_timestamp'] = time.time()
   
        N = len(data)
        # call_result_objs = []
        input_path_list=[]
        stepFunc = stepFunctionbuilder.StateFunctionWrapper()
        stateMachineName = func.__name__+"-"+str(time.time())
        # create state machine
        state = stepFunc.stateBuildeer(func.__name__,instance_input)
        print("state machine definition :<<<<<<<<<<<<<<<<<<<"+state)
        # raise Exception("eiei")
        stepFuncRole = "arn:aws:iam::{}:role/{}".format(self.config['account']['aws_account_id'], self.config['account']['aws_sfn_role'])

        stateMachine = stepFunc.create_state_machine(stateMachineName,str(state),stepFuncRole)
        self.stateMachine_arn = stateMachine
        # make a call here
        for i in range(N):
            if s3_file_url == False:
                print("s3_file_url >>>>>>>",s3_file_url)
                call_id = "{:05d}".format(i)

                # data_byte_range = None
                # if agg_data_key is not None:
                #     data_byte_range = agg_data_ranges[i]
                # put input to s3
                
                path = self.storage.predefine_put_data(call_id,timeStampId,func.__name__)
                input_path_list.append(path)
                # print(data[i])
                # print(path)
                # print(call_id)
                
                input_data_lambda = self.storage.contruct_input_json(data[i],call_id)
                # print("input data >>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<,")
                # print(input_data_lambda)
                
                input_data_lambda = json.dumps(input_data_lambda)
                # raise Exception("eieeiieieei")
                self.storage.put_data(path,input_data_lambda)
            else:
                call_id = "{:05d}".format(i)
                input_path_list.append(data[i])
            
            # cb = pool.apply_async(invoke, (data_strs[i], callset_id,
            #                                call_id, func_key,
            #                                host_job_meta.copy(),
            #                                agg_data_key,
            #                                data_byte_range))

            logger.info("map {} {} apply async".format(callset_id, call_id))

            # call_result_objs.append(cb)
        input_list=[]
        # invoke
        for index in input_path_list:
            inputS = stepFunc.contruct_statemachine_input(index,call_id)
            input_list.append(index)
            arn = stepFunc.create_execution(stateMachine,inputS)
        
        futureState = ResponseStateFuture(data, self.storage_path,stateMachine,self.storage,input_list)
        self.output_path = input_list
        # res = [c.get() for c in call_result_objs]
        # pool.close()
        # pool.join()
        logger.info("map invoked {} {} pool join".format(callset_id, call_id))

        # FIXME take advantage of the callset to return a lot of these

        # note these are just the invocation futures

        return futureState


    def reduce(self, function, list_of_futures,
               extra_env=None, extra_meta=None):
        """
        Apply a function across all futures.

        # FIXME change to lazy iterator
        """
        #if self.invoker.TIME_LIMIT:
        # stepFunc.wait
        # avoid race condition
        stepFunc = stepFunctionbuilder.StateFunctionWrapper()        
        if(self.stateMachine_arn==""):
            raise ValueError("State Machine ARN not found")
        stepFunc.wait(self.stateMachine_arn,self.input_list,stepFunc.ALL_COMPLETED)

        def reduce_func(fut_list):
            # FIXME speed this up for big reduce
            accum_list = []
            for f in fut_list:
                accum_list.append(f.result())
            return function(accum_list)

        return self.call_async(reduce_func, list_of_futures,
                               extra_env=extra_env, extra_meta=extra_meta)
    def reducer(self, function, list_of_futures,
               extra_env=None, extra_meta=None):
        """
        Apply a function across all futures.

        # FIXME change to lazy iterator
        """
        #if self.invoker.TIME_LIMIT:
        # stepFunc.wait
        # avoid race condition
        # new_input = []
        # new_input.append(list_of_futures.result_state())


        return self.call_async(function, list_of_futures.result_state(),
                               extra_env=extra_env, extra_meta=extra_meta)

    def get_logs(self, future, verbose=True):


        logclient = boto3.client('logs', region_name=self.config['account']['aws_region'])


        log_group_name = future.run_status['log_group_name']
        log_stream_name = future.run_status['log_stream_name']

        aws_request_id = future.run_status['aws_request_id']

        log_events = logclient.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name)

        # FIXME use logger
        if verbose:
            print("log events returned")
        this_events_logs = []
        in_this_event = False
        for event in log_events['events']:
            start_string = "START RequestId: {}".format(aws_request_id)
            end_string = "REPORT RequestId: {}".format(aws_request_id)

            message = event['message'].strip()
            timestamp = int(event['timestamp'])
            if verbose:
                print(timestamp, message)
            if start_string in message:
                in_this_event = True
            elif end_string in message:
                in_this_event = False
                this_events_logs.append((timestamp, message))

            if in_this_event:
                this_events_logs.append((timestamp, message))

        return this_events_logs
