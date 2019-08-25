import pywren
# from sympy import *
import numpy as np
import pandas as pd
import time
import uuid
import math
from io import StringIO

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
def uuidNaja(x):
    return uuid.uuid1().hex
def SortNumpy2(dataNaja):
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
def newSortNumpy(dataNaja):
    # print(dataNaja)
    csvFormatNaja = np.recfromcsv(StringIO(dataNaja), delimiter=',', names=['a', 'b', 'c','d'],encoding =None)
    # print(csvFormatNaja)
    csvFormatNaja.sort(axis=0,kind="mergesort",order="c")
    return csvFormatNaja
def FilterNaja7(inputList):
    df1 = pd.read_csv(StringIO(inputList[0]),sep=',')
    df2 = pd.read_csv(StringIO(inputList[1]),sep=',')
    df3 = df1.merge(df2, on=["ORDER_ID"], how='inner')
    priceFilter = df3["SHOP_PRICE"]>10
    df3.sort_values('ORDER_ID', inplace=True)
    
    # print(first)
    # .to_csv(index=False)
    resultFilter = df3[priceFilter]
    min= resultFilter['ORDER_ID'].min()
    max = resultFilter['ORDER_ID'].max()
    print(math.floor((min)/10000))
    print(math.ceil(max/10000))
    listLink =[]
    for i in range(math.floor((min)/10000),math.ceil(max/10000)+1):
        print(i)
        key = "{0}/{1}/{2}.csv".format(os.environ['AWS_LAMBDA_FUNCTION_NAME'],i,time.time())
        # print(key)
        # raise Exception("eieie")
        if min >0:
            resultFilter = resultFilter.loc[resultFilter['ORDER_ID']>i*10000]
        splitDf = resultFilter.loc[resultFilter['ORDER_ID']<(i+1)*10000].to_csv(index=False)
        s3obj = boto3.client('s3')
        s3obj.put_object(Bucket="output-bucky", Key=key, Body=splitDf)
        listLink.append(key)
    return listLink
def summer_reducer(x):
    return sum(x)
def MapperFunction(inputList):
    df1 = pd.read_csv(StringIO(inputList[0]),sep=',')
    df2 = pd.read_csv(StringIO(inputList[1]),sep=',')
    minORDER= df1['ORDER_ID'].min()
    minITEM = df2['ORDER_ID'].min()
    maxORDER= df1['ORDER_ID'].max()
    maxITEM = df2['ORDER_ID'].max()
    realMin = min(minITEM,maxORDER)
    realMax = max(maxORDER,maxITEM)
    outputORDERList = []
    outputITEMList = []
    partDf1 = df1
    partDf2 = df2
    for i in range(math.floor(minORDER/10000),math.ceil(maxORDER/10000)):
        tempO=[]
        
        tempO.append(i)
        if i >0:
            df1 = df1.loc[df1['ORDER_ID']>i*10000]
        partDf1 = df1.loc[df1['ORDER_ID']<(i+1)*10000].to_csv(index=False)
        tempO.append(partDf1)
        tempO.append("ORDER")
        # tempI.append(partDf2)
        outputORDERList.append(tempO)
        # outputITEMList.append(tempI)
    for i in range(math.floor(minITEM/10000),math.ceil(maxITEM/10000)):
        tempI = []
        tempI.append(i)
        if i >0:
            df2 = df2.loc[df2['ORDER_ID']>i*10000]
        partDf2 = df2.loc[df2['ORDER_ID']<(i+1)*10000].to_csv(index=False)
        tempI.append(partDf2)
        tempI.append("ITEM")
        # outputORDERList.append(tempO)
        outputITEMList.append(tempI)
    output = outputITEMList+outputORDERList
    return output
def ReduceFunc(inputList):
   

    framesItem = []
    framesOrder=[]
    for i in inputList:
        # print(i[0])
        if "ITEM" == i[0]:
            df = pd.read_csv(StringIO(i[1]),sep=",")
            framesItem.append(df)
        if "ORDER" == i[0]:
            df = pd.read_csv(StringIO(i[1]),sep=",")
            framesOrder.append(df)
    print(framesItem)
    print(framesOrder)
    dfITEM = pd.concat(framesItem)
    # print(dfITEM)
    dfORDER = pd.concat(framesOrder)
    # print(dfORDER)
    dfJoin = pd.merge(dfITEM,dfORDER, on="ORDER_ID", how='inner')
    # print(dfJoin)
    priceFilter = dfJoin["SHOP_PRICE"]>10
    dfJoin.sort_values('ORDER_ID', inplace=True)

    dfJoin = dfJoin[priceFilter]
    dfJoin['PAY_DT'] = pd.to_datetime(dfJoin['PAY_DT'], format="%d-%m-%Y %H:%M:%S")
    value_to_check2 = pd.Timestamp(2011, 5, 30)
    filterLessThan = dfJoin['PAY_DT']<value_to_check2
    dfJoin = dfJoin[filterLessThan]
    return len(dfJoin)
# dfx = pd.DataFrame({'date':['2015-02-21 12:08:51']})
# f1i1 = open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_1.csv", "r").read()
# f1i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_1.csv", "r").read()
# f2i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_2.csv", "r").read()
# f2i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_2.csv", "r").read()
# f3i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_3.csv", "r").read()
# f3i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_3.csv", "r").read()
# f4i1 = open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_1.csv", "r").read()
# f4i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_1.csv", "r").read()
# f5i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_2.csv", "r").read()
# f5i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_2.csv", "r").read()
# f6i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_3.csv", "r").read()
# f6i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_3.csv", "r").read()
# f7i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_2.csv", "r").read()
# f7i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_2.csv", "r").read()
# f8i1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/output_3.csv", "r").read()
# f8i2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output_ITEMS/output_3.csv", "r").read()
# def printEQ(x):
# f1= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/OS_ORDER_DUP_ITEM_1.csv", "r").read()
# f2= open("/media/p/Elements/downloads/ECT.tar/ECT/ECT/output/OS_ORDER_DUP1.csv", "r").read()
#     x, y, z, t = symbols('x y z t')
#     a = Rational(3,2)*pi + exp(I*x) / (x**2 + y)
#     return str(a)
# input_data = ['manualInput/output_1.csv']
# input_bench = [[f1i1,f1i2],[f2i1,f2i2],[f3i1,f3i2],[f4i1,f4i2],[f5i1,f5i2],[f6i1,f6i2],[f7i1,f7i2],[f8i1,f8i2]]
# input_bench = [[f1i1,f1i2]]
# input_bench2 = [[f2,f1]]
inputUrl = [['ReduceInputMapper/ITEM/output_1.csv','ReduceInputMapper/ORDER/output_1.csv'],['ReduceInputMapper/ITEM/output_2.csv','ReduceInputMapper/ORDER/output_2.csv'],['ReduceInputMapper/ITEM/output_3.csv','ReduceInputMapper/ORDER/output_3.csv'],['ReduceInputMapper/ITEM/output_4.csv','ReduceInputMapper/ORDER/output_4.csv'],['ReduceInputMapper/ITEM/output_5.csv','ReduceInputMapper/ORDER/output_5.csv'],['ReduceInputMapper/ITEM/output_6.csv','ReduceInputMapper/ORDER/output_6.csv'],['ReduceInputMapper/ITEM/output_7.csv','ReduceInputMapper/ORDER/output_7.csv'],['ReduceInputMapper/ITEM/output_8.csv','ReduceInputMapper/ORDER/output_8.csv'],['ReduceInputMapper/ITEM/output_9.csv','ReduceInputMapper/ORDER/output_9.csv'],['ReduceInputMapper/ITEM/output_10.csv','ReduceInputMapper/ORDER/output_10.csv'],['ReduceInputMapper/ITEM/output_11.csv','ReduceInputMapper/ORDER/output_11.csv'],['ReduceInputMapper/ITEM/output_12.csv','ReduceInputMapper/ORDER/output_12.csv'],['ReduceInputMapper/ITEM/output_13.csv','ReduceInputMapper/ORDER/output_13.csv'],['ReduceInputMapper/ITEM/output_14.csv','ReduceInputMapper/ORDER/output_14.csv'],['ReduceInputMapper/ITEM/output_15.csv','ReduceInputMapper/ORDER/output_15.csv'],['ReduceInputMapper/ITEM/output_16.csv','ReduceInputMapper/ORDER/output_16.csv'],['ReduceInputMapper/ITEM/output_17.csv','ReduceInputMapper/ORDER/output_17.csv'],['ReduceInputMapper/ITEM/output_18.csv','ReduceInputMapper/ORDER/output_18.csv'],['ReduceInputMapper/ITEM/output_19.csv','ReduceInputMapper/ORDER/output_19.csv'],['ReduceInputMapper/ITEM/output_20.csv','ReduceInputMapper/ORDER/output_20.csv'],
['ReduceInputMapper/ITEM/output_21.csv','ReduceInputMapper/ORDER/output_21.csv'],['ReduceInputMapper/ITEM/output_22.csv','ReduceInputMapper/ORDER/output_22.csv'],['ReduceInputMapper/ITEM/output_23.csv','ReduceInputMapper/ORDER/output_23.csv'],['ReduceInputMapper/ITEM/output_24.csv','ReduceInputMapper/ORDER/output_24.csv'],['ReduceInputMapper/ITEM/output_25.csv','ReduceInputMapper/ORDER/output_25.csv'],['ReduceInputMapper/ITEM/output_26.csv','ReduceInputMapper/ORDER/output_26.csv'],['ReduceInputMapper/ITEM/output_27.csv','ReduceInputMapper/ORDER/output_27.csv'],['ReduceInputMapper/ITEM/output_28.csv','ReduceInputMapper/ORDER/output_28.csv'],['ReduceInputMapper/ITEM/output_29.csv','ReduceInputMapper/ORDER/output_29.csv'],['ReduceInputMapper/ITEM/output_30.csv','ReduceInputMapper/ORDER/output_30.csv'],['ReduceInputMapper/ITEM/output_31.csv','ReduceInputMapper/ORDER/output_31.csv'],['ReduceInputMapper/ITEM/output_32.csv','ReduceInputMapper/ORDER/output_32.csv'],['ReduceInputMapper/ITEM/output_33.csv','ReduceInputMapper/ORDER/output_33.csv'],['ReduceInputMapper/ITEM/output_34.csv','ReduceInputMapper/ORDER/output_34.csv'],['ReduceInputMapper/ITEM/output_35.csv','ReduceInputMapper/ORDER/output_35.csv'],['ReduceInputMapper/ITEM/output_36.csv','ReduceInputMapper/ORDER/output_36.csv'],['ReduceInputMapper/ITEM/output_37.csv','ReduceInputMapper/ORDER/output_37.csv'],['ReduceInputMapper/ITEM/output_38.csv','ReduceInputMapper/ORDER/output_38.csv'],['ReduceInputMapper/ITEM/output_39.csv','ReduceInputMapper/ORDER/output_39.csv'],['ReduceInputMapper/ITEM/output_40.csv','ReduceInputMapper/ORDER/output_40.csv'],
['ReduceInputMapper/ITEM/output_41.csv','ReduceInputMapper/ORDER/output_41.csv'],['ReduceInputMapper/ITEM/output_42.csv','ReduceInputMapper/ORDER/output_42.csv'],['ReduceInputMapper/ITEM/output_43.csv','ReduceInputMapper/ORDER/output_43.csv'],['ReduceInputMapper/ITEM/output_44.csv','ReduceInputMapper/ORDER/output_44.csv'],['ReduceInputMapper/ITEM/output_45.csv','ReduceInputMapper/ORDER/output_45.csv'],['ReduceInputMapper/ITEM/output_46.csv','ReduceInputMapper/ORDER/output_46.csv'],['ReduceInputMapper/ITEM/output_47.csv','ReduceInputMapper/ORDER/output_47.csv'],['ReduceInputMapper/ITEM/output_48.csv','ReduceInputMapper/ORDER/output_48.csv'],['ReduceInputMapper/ITEM/output_49.csv','ReduceInputMapper/ORDER/output_49.csv'],['ReduceInputMapper/ITEM/output_50.csv','ReduceInputMapper/ORDER/output_50.csv'],['ReduceInputMapper/ITEM/output_51.csv','ReduceInputMapper/ORDER/output_51.csv'],['ReduceInputMapper/ITEM/output_52.csv','ReduceInputMapper/ORDER/output_52.csv'],['ReduceInputMapper/ITEM/output_53.csv','ReduceInputMapper/ORDER/output_53.csv'],['ReduceInputMapper/ITEM/output_54.csv','ReduceInputMapper/ORDER/output_54.csv'],['ReduceInputMapper/ITEM/output_55.csv','ReduceInputMapper/ORDER/output_55.csv'],['ReduceInputMapper/ITEM/output_56.csv','ReduceInputMapper/ORDER/output_56.csv'],['ReduceInputMapper/ITEM/output_57.csv','ReduceInputMapper/ORDER/output_57.csv'],['ReduceInputMapper/ITEM/output_58.csv','ReduceInputMapper/ORDER/output_58.csv'],['ReduceInputMapper/ITEM/output_59.csv','ReduceInputMapper/ORDER/output_59.csv'],['ReduceInputMapper/ITEM/output_60.csv','ReduceInputMapper/ORDER/output_60.csv']]

# print(input_bench)

exec = pywren.default_executor()
futures = exec.map(my_map3 , [1])
# futures = exec.map(FilterNaja5 , input_bench2,instance_specify="large")
t1 = time.time()
# futures = exec.map(MapperFunction , inputUrl,instance_specify="large",s3_file_url=True,is_url_list=True,intermediate_bucket='output-bucky')
map_done =time.time()
print("MAP DONE HERE<<<<<<<<<<<<<<<<<<<<<<<<<<")
print(map_done)
# input_data = ['InpuScaling128/output_1.csv','InpuScaling128/output_2.csv','InpuScaling128/output_3.csv','InpuScaling128/output_4.csv','InpuScaling128/output_5.csv','InpuScaling128/output_6.csv','InpuScaling128/output_7.csv','InpuScaling128/output_8.csv','InpuScaling128/output_9.csv','InpuScaling128/output_10.csv','InpuScaling128/output_11.csv','InpuScaling128/output_12.csv','InpuScaling128/output_13.csv','InpuScaling128/output_14.csv','InpuScaling128/output_15.csv','InpuScaling128/output_16.csv','InpuScaling128/output_17.csv','InpuScaling128/output_18.csv','InpuScaling128/output_19.csv','InpuScaling128/output_20.csv','InpuScaling128/output_21.csv','InpuScaling128/output_22.csv','InpuScaling128/output_23.csv','InpuScaling128/output_24.csv','InpuScaling128/output_25.csv','InpuScaling128/output_26.csv','InpuScaling128/output_27.csv','InpuScaling128/output_28.csv','InpuScaling128/output_29.csv','InpuScaling128/output_30.csv','InpuScaling128/output_31.csv','InpuScaling128/output_32.csv','InpuScaling128/output_33.csv','InpuScaling128/output_34.csv','InpuScaling128/output_35.csv','InpuScaling128/output_36.csv','InpuScaling128/output_37.csv','InpuScaling128/output_38.csv','InpuScaling128/output_39.csv','InpuScaling128/output_40.csv','InpuScaling128/output_41.csv','InpuScaling128/output_42.csv','InpuScaling128/output_43.csv','InpuScaling128/output_44.csv','InpuScaling128/output_45.csv','InpuScaling128/output_46.csv','InpuScaling128/output_47.csv','InpuScaling128/output_48.csv','InpuScaling128/output_49.csv','InpuScaling128/output_50.csv','InpuScaling128/output_51.csv','InpuScaling128/output_52.csv','InpuScaling128/output_53.csv','InpuScaling128/output_54.csv','InpuScaling128/output_55.csv','InpuScaling128/output_56.csv','InpuScaling128/output_57.csv','InpuScaling128/output_58.csv','InpuScaling128/output_59.csv','InpuScaling128/output_60.csv','InpuScaling128/output_61.csv','InpuScaling128/output_62.csv','InpuScaling128/output_63.csv','InpuScaling128/output_64.csv','InpuScaling128/output_65.csv','InpuScaling128/output_66.csv','InpuScaling128/output_67.csv','InpuScaling128/output_68.csv','InpuScaling128/output_69.csv','InpuScaling128/output_70.csv','InpuScaling128/output_71.csv','InpuScaling128/output_72.csv','InpuScaling128/output_73.csv','InpuScaling128/output_74.csv','InpuScaling128/output_75.csv','InpuScaling128/output_76.csv','InpuScaling128/output_77.csv','InpuScaling128/output_78.csv','InpuScaling128/output_79.csv','InpuScaling128/output_80.csv','InpuScaling128/output_81.csv','InpuScaling128/output_82.csv','InpuScaling128/output_83.csv','InpuScaling128/output_84.csv','InpuScaling128/output_85.csv','InpuScaling128/output_86.csv','InpuScaling128/output_87.csv','InpuScaling128/output_88.csv','InpuScaling128/output_89.csv','InpuScaling128/output_90.csv','InpuScaling128/output_91.csv','InpuScaling128/output_92.csv','InpuScaling128/output_93.csv','InpuScaling128/output_94.csv','InpuScaling128/output_95.csv','InpuScaling128/output_96.csv','InpuScaling128/output_97.csv','InpuScaling128/output_98.csv','InpuScaling128/output_99.csv','InpuScaling128/output_100.csv','InpuScaling128/output_101.csv','InpuScaling128/output_102.csv','InpuScaling128/output_103.csv','InpuScaling128/output_104.csv','InpuScaling128/output_105.csv','InpuScaling128/output_106.csv','InpuScaling128/output_107.csv','InpuScaling128/output_108.csv','InpuScaling128/output_109.csv','InpuScaling128/output_110.csv','InpuScaling128/output_111.csv','InpuScaling128/output_112.csv','InpuScaling128/output_113.csv','InpuScaling128/output_114.csv','InpuScaling128/output_115.csv','InpuScaling128/output_116.csv','InpuScaling128/output_117.csv','InpuScaling128/output_118.csv','InpuScaling128/output_119.csv','InpuScaling128/output_120.csv','InpuScaling128/output_121.csv','InpuScaling128/output_122.csv','InpuScaling128/output_123.csv','InpuScaling128/output_124.csv','InpuScaling128/output_125.csv','InpuScaling128/output_126.csv','InpuScaling128/output_127.csv','InpuScaling128/output_128.csv']
# futures = exec.map(newSortNumpy , input_data,s3_file_url=True)
# ,instance_specify="large",s3_file_url=True
# futures2 = exec.reducer(AggNaja,futures,instance_specify="medium")
# futures2 = exec.reducer(ReduceFunc,futures,instance_specify="large",maximumNumberOfWorker=120,is_shuffle=True)
reduce_done=time.time()
print("REDUCE DONE HERE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,")
print(reduce_done)
# future3 = exec.reducer(summer_reducer,futures2,instance_specify="small")
# pywren.wait(futures)
# wrenexec = pywren.default_executor()
# future = wrenexec.call_async(printEQ, 3)
# print("future here<<<<<<<<<<<<<<<<<<<<<<<")
# print(future)
# print("future here<<<<<<<<<<<<<<<<<<<<<<<")
# a = futures2.result_state(Mode="AWAY")
all_done = time.time()
total_time = all_done - t1
print("total time", total_time)
# a = futures.result_state()
# [0, 278696, 60928]
# print(a)
