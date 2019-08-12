import numpy as np
import pandas as pd
import time
import pywren
# import pandas as pd
# import pickle as pickle
# import click
# import pandas as pd
# import seaborn as sns
import uuid
import boto3
import rdsPostgresql

from pathlib import PurePath
from io import StringIO


# sns.set_style('whitegrid')

def compute_function(x):
    A = np.arange(1024**2, dtype=np.float64).reshape(1024, 1024)
    B = np.arange(1024**2, dtype=np.float64).reshape(1024, 1024)

    t1 = time.time()
    for i in range(10):
        c = np.sum(np.dot(A, B))

    FLOPS = 2 *  1024**3 * 10
    t2 = time.time()
    return FLOPS / (t2-t1)
    # A = np.arange(MAT_N**2, dtype=np.float64).reshape(MAT_N, MAT_N)
    # B = np.arange(MAT_N**2, dtype=np.float64).reshape(MAT_N, MAT_N)

    # t1 = time.time()
    # for i in range(loopcount):
    #     c = np.sum(np.dot(A, B))

    # FLOPS = 2 *  MAT_N**3 * loopcount
    # t2 = time.time()
    # return FLOPS / (t2-t1)
    
# loopcount, workers, matn, verbose=False



def benchmark(input,func,func_reduce):
    t1 = time.time()
    N = 3
    uuid1 = uuid.uuid1().hex
    print("3++++++++++++++++")
    # print(uuid)
    # print("<<<<<<<<<<<")
    sessionId = uuid.uuid3(uuid.NAMESPACE_DNS, func.__name__+uuid1)
    print("sessionId NAJA :",sessionId)
    engine = rdsPostgresql.initEngine()
    rdsPostgresql.insertTable(engine,uuid1,str(t1),"LightWeightStartExecution",sessionId,'-')
    print("4+++++++++++++++++++")
    # dynamodb = boto3.resource('dynamodb')
# dynamodb.put_item(TableName='TransactionLogger', Item={'fruitName':{'S':'Banana'},'key2':{'N':123},'UUID':{'S',a},'TimpStamp':{'S',str(time.time())}})
    # table = dynamodb.Table('TransactionLogger')
    # iters = np.arange(N)
    # table.put_item(
    # Item={
    #     'UUID': uuid,
    #     'TimeStamp': str(t1),
    #     'EventName': 'LightWeightStartExecution',
    #     'SessionId': sessionId,
    #     'StateId': '-',
    # }
    # )
    # def f(x):
    #     return {'flops': compute_flops(compute_function, matn)}
    print(input)
    print("5++++++++++++")
    pwex = pywren.lambda_executor()
    print("6++++++++++++")
    # raise Exception("aaa")
    futures = pwex.map(func, input,instance_specify="small")
    futures2 = pwex.reducer(func_reduce,futures,instance_specify="medium")
    print("7+++++++++++++++++")
    results =futures2.result_state()
    print(results)
    print("8++++++++++++++++++=")

    # print("invocation done, dur=", time.time() - t1)
    # # print("callset id: ", futures[0].callset_id) 
    # for i in range(500):
    #      for k in range(500):
    #         #  for x in range(500):
    #             continue
    all_done = time.time()
    uuid2 = uuid.uuid1().hex
    rdsPostgresql.insertTable(engine,uuid2,str(all_done),"LightWeightCompleteExecution",sessionId,'-')
    # table.put_item(
    # Item={
    #     'UUID': uuid2,
    #     'TimeStamp': str(all_done),
    #     'EventName': 'LightWeightCompleteExecution',
    #     'SessionId': sessionId,
    #     'StateId': '-',
    # }
    # )
   
    total_time = all_done - t1
    print("total time", total_time)
    est_flop = N * 2 * 10 * (1024 ** 3)

    # print(est_flop / 1e9 / total_time, "GFLOPS")
    # res = {'total_time': total_time,
    #        'est_flop': est_flop,
    #        'local_jobs_done_timeline': local_jobs_done_timeline,
    #        'results': results}
    return "Complete"


def results_to_dataframe(benchmark_data):
    callset_id = benchmark_data['callset_id']

    func_df = pd.DataFrame(benchmark_data['results']).rename(columns={'flops': 'intra_func_flops'})
    statuses_df = pd.DataFrame(benchmark_data['run_statuses'])
    invoke_df = pd.DataFrame(benchmark_data['invoke_statuses'])

    est_total_flops = benchmark_data['est_flop'] / benchmark_data['workers']
    results_df = pd.concat([statuses_df, invoke_df, func_df], axis=1)
    Cols = list(results_df.columns)
    for i, item in enumerate(results_df.columns):
        if item in results_df.columns[:i]: Cols[i] = "toDROP"
    results_df.columns = Cols
    results_df = results_df.drop("toDROP", 1)
    results_df['workers'] = benchmark_data['workers']
    results_df['loopcount'] = benchmark_data['loopcount']
    results_df['MATN'] = benchmark_data['MATN']
    results_df['est_flops'] = est_total_flops
    return results_df

def ON(x):
    sum=0
    for i in x:
        sum = sum+i
    return "Success NAlgor"
def ON2(x):
    sum=0
    for i in x:
        for j in x:
            sum=sum+i+j
    return "Success On2"
def ON3(x):
    sum = 0
    for i in x:
        for j in x:
            for k in x:
                sum = sum+i+j+k
    return "Success ON3"
def Fibonacci2N(n): 
    if n<0: 
        print("Incorrect input") 
    # First Fibonacci number is 0 
    elif n==1: 
        return 0
    # Second Fibonacci number is 1 
    elif n==2: 
        return 1
    else: 
        return Fibonacci2N(n-1)+Fibonacci2N(n-2)
def sortDataFrame(n):
    n.columns = ['a', 'b','c','d']
    n.sort_values(by='a',inplace= True, na_position ='last',kind="heapsort")
def sortNumpy(n):
    n.sort(axis=0,kind="mergesort")
    return n
def newSortNumpy(dataNaja):
    # print(dataNaja)
    csvFormatNaja = np.recfromcsv(StringIO(dataNaja), delimiter=',', names=['a', 'b', 'c','d'],encoding =None)
    # print(csvFormatNaja)
    csvFormatNaja.sort(axis=0,kind="mergesort",order="c")
    return csvFormatNaja

def FilterNaja(inputList):
    df1 = pd.read_csv(StringIO(inputList[0]),sep='\t')
    df2 = pd.read_csv(StringIO(inputList[1]),sep='\t')
    df3 = df1.merge(df2, on=["ORDER_ID"], how='inner')
    priceFilter = df3["SHOP_PRICE"]>10
    result = df3[priceFilter].to_csv(index=False)
    return result
def AggNaja(inputList):
    frames = []
    for i in inputList:
        df = pd.read_csv(StringIO(i),sep=",")
        frames.append(df)
    dfResult = pd.concat(frames)
    dfResult['PAY_DT'] = pd.to_datetime(dfResult['PAY_DT'], format="%d-%m-%Y %H:%M:%S")
    value_to_check2 = pd.Timestamp(2011, 5, 30)
    filterLessThan = dfResult['PAY_DT']<value_to_check2
    dfResult = dfResult[filterLessThan]
    return len(dfResult)

# worker scaling
# df = pd.read_csv('/home/p/Documents/fileSpilt/workerScale7.csv',header=None, skipinitialspace=True)
# See the keys
# numberOfWorker =3
# divider = len(df.index)/numberOfWorker
# print(len(df.index))
# chunks= []
# isFirst = True

# for i in range(0,numberOfWorker):
#     divider = len(df.index)/numberOfWorker
#     # 12/3 = 4
#     #4-8 =1*4 + 2*4
#     #8-12= 2*4 + 3*4
#     if isFirst == True:
#         chunks.append(df.iloc[:int(divider), :])
#         isFirst = False
#         continue
#     chunks.append(df.iloc[int(divider)*i:int(divider)*(i+1), :])
# listdd = []
# listdd.append(chunks[0])
# import sys
# print(sys.getsizeof(chunks[0]))
# print(len(listdd))

# inputNAlgorFirst = list(range(0,100))
# inputFibo = [2]
# inputNAlgor = [inputNAlgorFirst,inputNAlgorFirst,inputNAlgorFirst,inputNAlgorFirst,inputNAlgorFirst]
# print(inputNAlgor)
# print(len(inputNAlgor))
# raise Exception("eieiee")
# print("<<<>>>>>")
# my_data = np.recfromcsv('/home/p/Documents/fileSpilt/workerScale800.csv', delimiter=',', names=['a', 'b', 'c','d'],encoding =None)
# print("1<<<<<<<")
# spilt =  np.array_split(my_data, 8)
# print("2<<<<<<<<<<<")
#input_data = ['InpuScaling128/output_1.csv','InpuScaling128/output_2.csv','InpuScaling128/output_3.csv','InpuScaling128/output_4.csv','InpuScaling128/output_5.csv','InpuScaling128/output_6.csv','InpuScaling128/output_7.csv','InpuScaling128/output_8.csv','InpuScaling128/output_9.csv','InpuScaling128/output_10.csv','InpuScaling128/output_11.csv','InpuScaling128/output_12.csv','InpuScaling128/output_13.csv','InpuScaling128/output_14.csv','InpuScaling128/output_15.csv','InpuScaling128/output_16.csv','InpuScaling128/output_17.csv','InpuScaling128/output_18.csv','InpuScaling128/output_19.csv','InpuScaling128/output_20.csv','InpuScaling128/output_21.csv','InpuScaling128/output_22.csv','InpuScaling128/output_23.csv','InpuScaling128/output_24.csv','InpuScaling128/output_25.csv','InpuScaling128/output_26.csv','InpuScaling128/output_27.csv','InpuScaling128/output_28.csv','InpuScaling128/output_29.csv','InpuScaling128/output_30.csv','InpuScaling128/output_31.csv','InpuScaling128/output_32.csv','InpuScaling128/output_33.csv','InpuScaling128/output_34.csv','InpuScaling128/output_35.csv','InpuScaling128/output_36.csv','InpuScaling128/output_37.csv','InpuScaling128/output_38.csv','InpuScaling128/output_39.csv','InpuScaling128/output_40.csv','InpuScaling128/output_41.csv','InpuScaling128/output_42.csv','InpuScaling128/output_43.csv','InpuScaling128/output_44.csv','InpuScaling128/output_45.csv','InpuScaling128/output_46.csv','InpuScaling128/output_47.csv','InpuScaling128/output_48.csv','InpuScaling128/output_49.csv','InpuScaling128/output_50.csv','InpuScaling128/output_51.csv','InpuScaling128/output_52.csv','InpuScaling128/output_53.csv','InpuScaling128/output_54.csv','InpuScaling128/output_55.csv','InpuScaling128/output_56.csv','InpuScaling128/output_57.csv','InpuScaling128/output_58.csv','InpuScaling128/output_59.csv','InpuScaling128/output_60.csv','InpuScaling128/output_61.csv','InpuScaling128/output_62.csv','InpuScaling128/output_63.csv','InpuScaling128/output_64.csv','InpuScaling128/output_65.csv','InpuScaling128/output_66.csv','InpuScaling128/output_67.csv','InpuScaling128/output_68.csv','InpuScaling128/output_69.csv','InpuScaling128/output_70.csv','InpuScaling128/output_71.csv','InpuScaling128/output_72.csv','InpuScaling128/output_73.csv','InpuScaling128/output_74.csv','InpuScaling128/output_75.csv','InpuScaling128/output_76.csv','InpuScaling128/output_77.csv','InpuScaling128/output_78.csv','InpuScaling128/output_79.csv','InpuScaling128/output_80.csv','InpuScaling128/output_81.csv','InpuScaling128/output_82.csv','InpuScaling128/output_83.csv','InpuScaling128/output_84.csv','InpuScaling128/output_85.csv','InpuScaling128/output_86.csv','InpuScaling128/output_87.csv','InpuScaling128/output_88.csv','InpuScaling128/output_89.csv','InpuScaling128/output_90.csv','InpuScaling128/output_91.csv','InpuScaling128/output_92.csv','InpuScaling128/output_93.csv','InpuScaling128/output_94.csv','InpuScaling128/output_95.csv','InpuScaling128/output_96.csv','InpuScaling128/output_97.csv','InpuScaling128/output_98.csv','InpuScaling128/output_99.csv','InpuScaling128/output_100.csv','InpuScaling128/output_101.csv','InpuScaling128/output_102.csv','InpuScaling128/output_103.csv','InpuScaling128/output_104.csv','InpuScaling128/output_105.csv','InpuScaling128/output_106.csv','InpuScaling128/output_107.csv','InpuScaling128/output_108.csv','InpuScaling128/output_109.csv','InpuScaling128/output_110.csv','InpuScaling128/output_111.csv','InpuScaling128/output_112.csv','InpuScaling128/output_113.csv','InpuScaling128/output_114.csv','InpuScaling128/output_115.csv','InpuScaling128/output_116.csv','InpuScaling128/output_117.csv','InpuScaling128/output_118.csv','InpuScaling128/output_119.csv','InpuScaling128/output_120.csv','InpuScaling128/output_121.csv','InpuScaling128/output_122.csv','InpuScaling128/output_123.csv','InpuScaling128/output_124.csv','InpuScaling128/output_125.csv','InpuScaling128/output_126.csv','InpuScaling128/output_127.csv','InpuScaling128/output_128.csv']
# input_data=['TestNaja/inputScale1MB.csv']
# input_data = ['InpuScaling128/output_1.csv','InpuScaling128/output_2.csv','InpuScaling128/output_3.csv','InpuScaling128/output_4.csv','InpuScaling128/output_5.csv','InpuScaling128/output_6.csv','InpuScaling128/output_7.csv','InpuScaling128/output_8.csv','InpuScaling128/output_9.csv','InpuScaling128/output_10.csv','InpuScaling128/output_11.csv','InpuScaling128/output_12.csv','InpuScaling128/output_13.csv','InpuScaling128/output_14.csv','InpuScaling128/output_15.csv','InpuScaling128/output_16.csv','InpuScaling128/output_17.csv','InpuScaling128/output_18.csv','InpuScaling128/output_19.csv','InpuScaling128/output_20.csv','InpuScaling128/output_21.csv','InpuScaling128/output_22.csv','InpuScaling128/output_23.csv','InpuScaling128/output_24.csv','InpuScaling128/output_25.csv','InpuScaling128/output_26.csv','InpuScaling128/output_27.csv','InpuScaling128/output_28.csv','InpuScaling128/output_29.csv','InpuScaling128/output_30.csv','InpuScaling128/output_31.csv','InpuScaling128/output_32.csv','InpuScaling128/output_33.csv','InpuScaling128/output_34.csv','InpuScaling128/output_35.csv','InpuScaling128/output_36.csv','InpuScaling128/output_37.csv','InpuScaling128/output_38.csv','InpuScaling128/output_39.csv','InpuScaling128/output_40.csv','InpuScaling128/output_41.csv','InpuScaling128/output_42.csv','InpuScaling128/output_43.csv','InpuScaling128/output_44.csv','InpuScaling128/output_45.csv','InpuScaling128/output_46.csv','InpuScaling128/output_47.csv','InpuScaling128/output_48.csv','InpuScaling128/output_49.csv','InpuScaling128/output_50.csv','InpuScaling128/output_51.csv','InpuScaling128/output_52.csv','InpuScaling128/output_53.csv','InpuScaling128/output_54.csv','InpuScaling128/output_55.csv','InpuScaling128/output_56.csv','InpuScaling128/output_57.csv','InpuScaling128/output_58.csv','InpuScaling128/output_59.csv','InpuScaling128/output_60.csv','InpuScaling128/output_61.csv','InpuScaling128/output_62.csv','InpuScaling128/output_63.csv','InpuScaling128/output_64.csv']
# input_data = ['manualInput/output_1.csv','manualInput/output_2.csv','manualInput/output_3.csv','manualInput/output_4.csv','manualInput/output_5.csv','manualInput/output_6.csv','manualInput/output_7.csv','manualInput/output_8.csv','manualInput/output_9.csv','manualInput/output_10.csv','manualInput/output_11.csv','manualInput/output_12.csv','manualInput/output_13.csv','manualInput/output_14.csv','manualInput/output_15.csv','manualInput/output_16.csv','manualInput/output_17.csv','manualInput/output_18.csv','manualInput/output_19.csv','manualInput/output_20.csv','manualInput/output_21.csv','manualInput/output_22.csv','manualInput/output_23.csv','manualInput/output_24.csv','manualInput/output_25.csv','manualInput/output_26.csv','manualInput/output_27.csv','manualInput/output_28.csv','manualInput/output_29.csv','manualInput/output_30.csv','manualInput/output_31.csv','manualInput/output_32.csv']
# input_data = ['manualInput/output_1.csv']
f1i1 = open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_1.csv", "r").read()
f1i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_1.csv", "r").read()
f2i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_2.csv", "r").read()
f2i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_2.csv", "r").read()
f3i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_3.csv", "r").read()
f3i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_3.csv", "r").read()
f4i1 = open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_1.csv", "r").read()
f4i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_1.csv", "r").read()
f5i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_2.csv", "r").read()
f5i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_2.csv", "r").read()
f6i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_3.csv", "r").read()
f6i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_3.csv", "r").read()
f7i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_2.csv", "r").read()
f7i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_2.csv", "r").read()
f8i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_3.csv", "r").read()
f8i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_3.csv", "r").read()
input_bench = [[f1i1,f1i2],[f2i1,f2i2],[f3i1,f3i2],[f4i1,f4i2],[f5i1,f5i2],[f6i1,f6i2],[f7i1,f7i2],[f8i1,f8i2]]
a = benchmark(input_bench,FilterNaja,AggNaja)
print(a)

# @click.command()
# @click.option('--workers', default=10, help='how many workers', type=int)
# @click.option('--outfile', default='flops_benchmark.pickle',
#               help='filename to save results in')
# @click.option('--loopcount', default=6, help='Number of matmuls to do.', type=int)
# @click.option('--matn', default=1024, help='size of matrix', type=int)
# def run_benchmark(workers, outfile, loopcount, matn):
#     res = benchmark(loopcount, workers, matn)
#     res['loopcount'] = loopcount
#     res['workers'] = workers
#     res['MATN'] = matn

#     pickle.dump(res, open(outfile, 'wb'), -1)


# if __name__ == "__main__":
#     run_benchmark()
