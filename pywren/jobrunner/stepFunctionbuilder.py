from botocore import session
import json
import datetime
from datetime import tzinfo
from dateutil.tz import *
import time


class StateFunctionWrapper(object):
    """A wrapper for State Function"""
    def __init__(self, *args, **kwargs):
        self._session = session.get_session()
        self.client = self._session.create_client(
            'stepfunctions', region_name="eu-central-1")
        self.ALL_COMPLETED = 1
        self.ANY_COMPLETED = 2
        self.ALWAYS = 3
        

    def buildStateChecker(self,arn,statusFilter=None):
      stateList = self.get_execution(arn,statusFilter)
      retryList = self.reason_failure(stateList)
    #   print(retryList)

    def wait(self,arn,future,returnType,waitDuration=10):
      succeedStateMachineList=[]
      failedStateMachineList=[]
      uncompletedStateMachineList=[]
      numberOfTask = len(future)
      numberOfCompletedTask=0
      if returnType == self.ALL_COMPLETED:
        while True:
            data = self.get_execution(arn)
            # data = {'executions': [{'executionArn': 'arn:aws:states:eu-central-1:251584899486:execution:my_map_function3-1554164784.7738473:c046f37b-93b8-4db1-b155-533e0cf59a73', 'stateMachineArn': 'arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473', 'name': 'c046f37b-93b8-4db1-b155-533e0cf59a73', 'status': 'RUNNING', 'startDate': datetime.datetime(2019, 4, 2, 7, 26, 25, 46000, tzinfo=tzlocal()), 'stopDate': datetime.datetime(2019, 4, 2, 7, 27, 55, 927000, tzinfo=tzlocal())}, {'executionArn': 'arn:aws:states:eu-central-1:251584899486:execution:my_map_function3-1554164784.7738473:e292748b-3abd-412b-add1-62e143cff897', 'stateMachineArn': 'arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473', 'name': 'e292748b-3abd-412b-add1-62e143cff897', 'status': 'FAILED', 'startDate': datetime.datetime(2019, 4, 2, 7, 26, 25, 127000, tzinfo=tzlocal()), 'stopDate': datetime.datetime(2019, 4, 2, 7, 27, 55, 915000, tzinfo=tzlocal())}, {'executionArn': 'arn:aws:states:eu-central-1:251584899486:execution:my_map_function3-1554164784.7738473:4cd7a938-0486-4722-8888-bc36ed0a7991', 'stateMachineArn': 'arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473', 'name': '4cd7a938-0486-4722-8888-bc36ed0a7991', 'status': 'FAILED', 'startDate': datetime.datetime(2019, 4, 2, 7, 26, 24, 951000, tzinfo=tzlocal()), 'stopDate': datetime.datetime(2019, 4, 2, 7, 27,
# 55, 781000, tzinfo=tzlocal())}], 'ResponseMetadata': {'RequestId': '4ece9e7b-5945-11e9-991b-4748fbc60275', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': '4ece9e7b-5945-11e9-991b-4748fbc60275', 'content-type': 'application/x-amz-json-1.0', 'content-length': '1138'}, 'RetryAttempts': 0}}
            print(data)

            for index in data['executions']:
                print(index)
                print(index['status'])
                if(index['status']=='SUCCEEDED'):
                    # print("here please") 
                    
                    if(index['executionArn'] not in succeedStateMachineList):
                        numberOfCompletedTask=numberOfCompletedTask+1
                        succeedStateMachineList.append(index['executionArn'])
                    if(numberOfCompletedTask==numberOfTask):
                        return succeedStateMachineList,failedStateMachineList,uncompletedStateMachineList                         
                elif(index['status']=='FAILED'):
                    # print("here please") 
                    if(index['executionArn'] not in failedStateMachineList):
                        numberOfCompletedTask=numberOfCompletedTask+1
                        failedStateMachineList.append(index['executionArn'])
                    if(numberOfCompletedTask==numberOfTask):
                        return succeedStateMachineList,failedStateMachineList,uncompletedStateMachineList 
                else:
                    # print("not here")
                    time.sleep(waitDuration)
      elif returnType == self.ANY_COMPLETED:
          while True:
            data = self.get_execution(arn)
            # data = {'executions': [{'executionArn': 'arn:aws:states:eu-central-1:251584899486:execution:my_map_function3-1554164784.7738473:c046f37b-93b8-4db1-b155-533e0cf59a73', 'stateMachineArn': 'arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473', 'name': 'c046f37b-93b8-4db1-b155-533e0cf59a73', 'status': 'RUNNING', 'startDate': datetime.datetime(2019, 4, 2, 7, 26, 25, 46000, tzinfo=tzlocal()), 'stopDate': datetime.datetime(2019, 4, 2, 7, 27, 55, 927000, tzinfo=tzlocal())}, {'executionArn': 'arn:aws:states:eu-central-1:251584899486:execution:my_map_function3-1554164784.7738473:e292748b-3abd-412b-add1-62e143cff897', 'stateMachineArn': 'arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473', 'name': 'e292748b-3abd-412b-add1-62e143cff897', 'status': 'RUNNING', 'startDate': datetime.datetime(2019, 4, 2, 7, 26, 25, 127000, tzinfo=tzlocal()), 'stopDate': datetime.datetime(2019, 4, 2, 7, 27, 55, 915000, tzinfo=tzlocal())}, {'executionArn': 'arn:aws:states:eu-central-1:251584899486:execution:my_map_function3-1554164784.7738473:4cd7a938-0486-4722-8888-bc36ed0a7991', 'stateMachineArn': 'arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473', 'name': '4cd7a938-0486-4722-8888-bc36ed0a7991', 'status': 'RUNNING', 'startDate': datetime.datetime(2019, 4, 2, 7, 26, 24, 951000, tzinfo=tzlocal()), 'stopDate': datetime.datetime(2019, 4, 2, 7, 27,
# 55, 781000, tzinfo=tzlocal())}], 'ResponseMetadata': {'RequestId': '4ece9e7b-5945-11e9-991b-4748fbc60275', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': '4ece9e7b-5945-11e9-991b-4748fbc60275', 'content-type': 'application/x-amz-json-1.0', 'content-length': '1138'}, 'RetryAttempts': 0}}
            print(data)
            for index in data['executions']:
                if(index['status']=='SUCCEEDED'):
                    
                    if(index['executionArn'] not in succeedStateMachineList):
                        succeedStateMachineList.append(index['executionArn'])                        
                elif(index['status']=='FAILED'):
                    # print("here please") 
                    if(index['executionArn'] not in failedStateMachineList):
                        failedStateMachineList.append(index['executionArn'])
                else:                    
                    if(index['executionArn'] not in uncompletedStateMachineList): 
                        uncompletedStateMachineList.append(index['executionArn'])
                    print(uncompletedStateMachineList)
            
            if len(succeedStateMachineList)>0 or len(failedStateMachineList)>0:
                return completedStateMachineList,uncompletedStateMachineList
            time.sleep(waitDuration)
      elif returnType == self.ALWAYS:
            data = self.get_execution(arn)
            # data = {'executions': [{'executionArn': 'arn:aws:states:eu-central-1:251584899486:execution:my_map_function3-1554164784.7738473:c046f37b-93b8-4db1-b155-533e0cf59a73', 'stateMachineArn': 'arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473', 'name': 'c046f37b-93b8-4db1-b155-533e0cf59a73', 'status': 'RUNNING', 'startDate': datetime.datetime(2019, 4, 2, 7, 26, 25, 46000, tzinfo=tzlocal()), 'stopDate': datetime.datetime(2019, 4, 2, 7, 27, 55, 927000, tzinfo=tzlocal())}, {'executionArn': 'arn:aws:states:eu-central-1:251584899486:execution:my_map_function3-1554164784.7738473:e292748b-3abd-412b-add1-62e143cff897', 'stateMachineArn': 'arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473', 'name': 'e292748b-3abd-412b-add1-62e143cff897', 'status': 'RUNNING', 'startDate': datetime.datetime(2019, 4, 2, 7, 26, 25, 127000, tzinfo=tzlocal()), 'stopDate': datetime.datetime(2019, 4, 2, 7, 27, 55, 915000, tzinfo=tzlocal())}, {'executionArn': 'arn:aws:states:eu-central-1:251584899486:execution:my_map_function3-1554164784.7738473:4cd7a938-0486-4722-8888-bc36ed0a7991', 'stateMachineArn': 'arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473', 'name': '4cd7a938-0486-4722-8888-bc36ed0a7991', 'status': 'RUNNING', 'startDate': datetime.datetime(2019, 4, 2, 7, 26, 24, 951000, tzinfo=tzlocal()), 'stopDate': datetime.datetime(2019, 4, 2, 7, 27,
# 55, 781000, tzinfo=tzlocal())}], 'ResponseMetadata': {'RequestId': '4ece9e7b-5945-11e9-991b-4748fbc60275', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': '4ece9e7b-5945-11e9-991b-4748fbc60275', 'content-type': 'application/x-amz-json-1.0', 'content-length': '1138'}, 'RetryAttempts': 0}}
            print(data)
            for index in data['executions']:
                if(index['status']=='SUCCEEDED'):        
                    if(index['executionArn'] not in succeedStateMachineList):
                        succeedStateMachineList.append(index['executionArn'])                        
                elif(index['status']=='FAILED'):
                    # print("here please") 
                    if(index['executionArn'] not in failedStateMachineList):
                        failedStateMachineList.append(index['executionArn'])
                else:uncompletedStateMachineList.append(index['executionArn'])

            return succeedStateMachineList,failedStateMachineList,uncompletedStateMachineList


    def reason_failure(self,data,includeException=False):
        reasonList= []
        for index in data['executions']:
            if(index['status']=='SUCCEEDED'):
                print('Succ')
            elif(index['status']=='FAILED'):
                response = self.client.get_execution_history(
                    executionArn=index['executionArn'],
                    maxResults=10,
                    reverseOrder=True
                    )
                reason = {"ErrorType":"","Description":"","Call_number":{}}
                for x in response['events']:
                    # print(x)
                    if x['type'] == 'ExecutionFailed':
                        reason["ErrorType"] = x['executionFailedEventDetails']['error']
                        reason['Description'] = x['executionFailedEventDetails']['cause']
                    elif x['type'] == 'LambdaFunctionScheduled':
                        reason["Call_number"] = x['lambdaFunctionScheduledEventDetails']["input"]
                        break
                reasonList.append(reason)
            

                
        print("<<<<<<")
        print(reasonList)
        return reasonList

    def input_path_builder(self,index):
        return "$.input["+str(index)+"]"

    def contruct_statemachine_input(self,inputExecutor,call_id):
        data={"input":inputExecutor,"call_id":call_id}
        return json.dumps(data)

    def stateBuildeer(self,func,instance_input):

        resource = "arn:aws:lambda:eu-central-1:251584899486:function:"+func+"-128"
        resource2 = "arn:aws:lambda:eu-central-1:251584899486:function:"+func+'-1600'
        resource3 = "arn:aws:lambda:eu-central-1:251584899486:function:"+func+'-2688'
        if instance_input == "small":
            data = {
    "Comment": "Executor",
    "StartAt": "Submit Job",
    "States": {
        "Submit Job": {
        "Type": "Task",
        "Resource":resource,
        "Catch": [   {
            "ErrorEquals": ["TimeoutError"],
            "ResultPath": "$.error-info",
            "Next": "ChangeInstance"
            }],
        "Retry": [
            {
            "ErrorEquals": ["States.Timeout"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.TaskFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.Permissions"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ResultPathMatchFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ParameterPathFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.BranchFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.NoChoiceMatched"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 5,
            "MaxAttempts": 2,
            "BackoffRate": 2.0
            }
        ],
        "End": True
        },
        "ChangeInstance": {
        "Type": "Task",
        "Resource":resource2,
        "Catch": [   {
            "ErrorEquals": ["TimeoutError"],
            "ResultPath": "$.error-info",
            "Next": "ChangeInstance2"
            }],
        "Retry": [
            {
            "ErrorEquals": ["States.Timeout"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.TaskFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.Permissions"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ResultPathMatchFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ParameterPathFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.BranchFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.NoChoiceMatched"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 5,
            "MaxAttempts": 2,
            "BackoffRate": 2.0
            }
        ],

        "End": True
        },
        "ChangeInstance2": {
        "Type": "Task",
        "Resource":resource3,
        "Catch": [   {
            "ErrorEquals": ["TimeoutError"],
            "ResultPath": "$.error-info",
            "Next": "FailState"
            }],
        "Retry": [
            {
            "ErrorEquals": ["States.Timeout"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.TaskFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.Timeout"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.Permissions"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ResultPathMatchFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ParameterPathFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.BranchFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.NoChoiceMatched"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 5,
            "MaxAttempts": 2,
            "BackoffRate": 2.0
            }
        ],
        "End": True
        },     "FailState": {
            "Type": "Fail",
            "Error": "TimeOut",
            "Cause": "Execution fail, it take more than 15 minites even for largest instance, please change input accordingly"
    }
    }
    }
        elif instance_input == "medium":
            data =  {
    "Comment": "Executor",
    "StartAt": "Submit Job",
    "States": {
        "Submit Job": {
        "Type": "Task",
        "Resource":resource2,
        "Catch": [   {
            "ErrorEquals": ["TimeoutError"],
            "ResultPath": "$.error-info",
            "Next": "ChangeInstance"
            }],
        "Retry": [
            {
            "ErrorEquals": ["States.Timeout"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.TaskFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.Permissions"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ResultPathMatchFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ParameterPathFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.BranchFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.NoChoiceMatched"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 5,
            "MaxAttempts": 2,
            "BackoffRate": 2.0
            }
        ],
        "End": True
        },
        "ChangeInstance": {
        "Type": "Task",
        "Resource":resource3,
        "Catch": [   {
            "ErrorEquals": ["TimeoutError"],
            "ResultPath": "$.error-info",
            "Next": "FailState"
            }],
        "Retry": [
            {
            "ErrorEquals": ["States.Timeout"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.TaskFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.Timeout"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.Permissions"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ResultPathMatchFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ParameterPathFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.BranchFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.NoChoiceMatched"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 5,
            "MaxAttempts": 2,
            "BackoffRate": 2.0
            }
        ],
        "End": True
        },     "FailState": {
            "Type": "Fail",
            "Error": "TimeOut",
            "Cause": "Execution fail, it take more than 15 minites even for largest instance, please change input accordingly"
    }
    }
    }
        elif instance_input == "large":
            data =  {
    "Comment": "Executor",
    "StartAt": "Submit Job",
    "States": {
        "Submit Job": {
        "Type": "Task",
        "Resource":resource3,
        "Catch": [   {
            "ErrorEquals": ["TimeoutError"],
            "ResultPath": "$.error-info",
            "Next": "FailState"
            }],
        "Retry": [
            {
            "ErrorEquals": ["States.Timeout"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.TaskFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.Permissions"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ResultPathMatchFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ParameterPathFailure"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.BranchFailed"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.NoChoiceMatched"],
            "MaxAttempts": 0
            },
            {
            "ErrorEquals": ["States.ALL"],
            "IntervalSeconds": 5,
            "MaxAttempts": 2,
            "BackoffRate": 2.0
            }
        ],
        "End": True
        },
            "FailState": {
            "Type": "Fail",
            "Error": "TimeOut",
            "Cause": "Execution fail, it take more than 15 minites even for largest instance, please change input accordingly"
    }
    }
    }
        json_data = json.dumps(data)
        # print(json_data)
        # raise Exception("eieie")
        return json_data

    def create_state_machine(self, name, definition, role_arn):
        """
        Create a state machine.
        PARAMS
        @name: name of the state machine
        @defination: json definition of the state machine
        @role_arn: Arn of the role created for this state machine
        """
        response = self.client.create_state_machine(
            name=name,
            definition=definition,
            roleArn=role_arn
        )
        sm_arn = response.get('stateMachineArn')
        if sm_arn:
            print('State Machine {0} with arn {1} created successfully'.format(
                name, sm_arn
            ))
        return sm_arn

    def get_state_machine(self, name):
        """
        Get a state machine given its name
        """
        response = self.client.list_state_machines()
        print(response)
        if not response.get('stateMachines'):
            return None
        for sm in response.get('stateMachines'):
            if sm['name'] == name:
                return sm['stateMachineArn']

    def get_execution(self,arn,statusFilter=None):
        if(statusFilter!=None):
          response = self.client.list_executions(arn,statusFilter)
        else: response = self.client.list_executions(stateMachineArn=arn)
        return response

    def create_execution(self, sm_arn, input_data):
        """
        Create an execution for a state machine.
        PARAMS
        @sm_arn: Arn of the state machine that is to be executed
        @input_data: Input json data to be passed for the execution
        """
        execution_response = self.client.start_execution(
            stateMachineArn=sm_arn,
            input=input_data
        )
        execution_arn = execution_response.get('executionArn')
        return execution_arn

    def dummy_state_machine(self, sm_name, lambda_1_arn, lambda_2_arn):
        """
        Create a dummy state machine
        https://states-language.net/spec.html
        """
        state_function_definition = {
            "Comment": "A dummy state machine",
            "StartAt": "State1",
            "States": {
                "State1": {
                    "Resource": lambda_1_arn,
                    "Type": "Task",
                    "Next": "State2"
                },
                "State2": {
                    "Type": "Task",
                    "Resource": lambda_2_arn,
                    "Next": "End"
                },
                "End": {
                    "Type": "Succeed"
                }
            }
        }

        with open('/tmp/dummy-sm.json', 'wb') as jsonfile:
            json.dump(state_function_definition, jsonfile, indent=4)
        self.create_state_machine(
            name=sm_name, definition=state_function_definition
        )
# a =StateFunctionWrapper()
# x=a.get_execution("arn:aws:states:eu-central-1:251584899486:stateMachine:numpy_test-1555951640.059513")
# print(x)
# response = self.client.get_execution_history(
#                     executionArn=index['executionArn'],
#                     maxResults=10,
#                     reverseOrder=True
#                     )
# buildStateChecker('','arn:aws:states:eu-central-1:251584899486:stateMachine:my_map_function3-1554164784.7738473','')
