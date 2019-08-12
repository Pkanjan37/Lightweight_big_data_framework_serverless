import pywren
# from sympy import *
import numpy as np
import pandas as pd
import time
import uuid
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
dfx = pd.DataFrame({'date':['2015-02-21 12:08:51']})
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
# def printEQ(x):
#     x, y, z, t = symbols('x y z t')
#     a = Rational(3,2)*pi + exp(I*x) / (x**2 + y)
#     return str(a)
# input_data = ['manualInput/output_1.csv']
# input_bench = [[f1i1,f1i2],[f2i1,f2i2],[f3i1,f3i2],[f4i1,f4i2],[f5i1,f5i2],[f6i1,f6i2],[f7i1,f7i2],[f8i1,f8i2]]
input_bench = [[f1i1,f1i2]]

# print(input_bench)

exec = pywren.default_executor()
futures = exec.map(FilterNaja , input_bench)

# input_data = ['InpuScaling128/output_1.csv','InpuScaling128/output_2.csv','InpuScaling128/output_3.csv','InpuScaling128/output_4.csv','InpuScaling128/output_5.csv','InpuScaling128/output_6.csv','InpuScaling128/output_7.csv','InpuScaling128/output_8.csv','InpuScaling128/output_9.csv','InpuScaling128/output_10.csv','InpuScaling128/output_11.csv','InpuScaling128/output_12.csv','InpuScaling128/output_13.csv','InpuScaling128/output_14.csv','InpuScaling128/output_15.csv','InpuScaling128/output_16.csv','InpuScaling128/output_17.csv','InpuScaling128/output_18.csv','InpuScaling128/output_19.csv','InpuScaling128/output_20.csv','InpuScaling128/output_21.csv','InpuScaling128/output_22.csv','InpuScaling128/output_23.csv','InpuScaling128/output_24.csv','InpuScaling128/output_25.csv','InpuScaling128/output_26.csv','InpuScaling128/output_27.csv','InpuScaling128/output_28.csv','InpuScaling128/output_29.csv','InpuScaling128/output_30.csv','InpuScaling128/output_31.csv','InpuScaling128/output_32.csv','InpuScaling128/output_33.csv','InpuScaling128/output_34.csv','InpuScaling128/output_35.csv','InpuScaling128/output_36.csv','InpuScaling128/output_37.csv','InpuScaling128/output_38.csv','InpuScaling128/output_39.csv','InpuScaling128/output_40.csv','InpuScaling128/output_41.csv','InpuScaling128/output_42.csv','InpuScaling128/output_43.csv','InpuScaling128/output_44.csv','InpuScaling128/output_45.csv','InpuScaling128/output_46.csv','InpuScaling128/output_47.csv','InpuScaling128/output_48.csv','InpuScaling128/output_49.csv','InpuScaling128/output_50.csv','InpuScaling128/output_51.csv','InpuScaling128/output_52.csv','InpuScaling128/output_53.csv','InpuScaling128/output_54.csv','InpuScaling128/output_55.csv','InpuScaling128/output_56.csv','InpuScaling128/output_57.csv','InpuScaling128/output_58.csv','InpuScaling128/output_59.csv','InpuScaling128/output_60.csv','InpuScaling128/output_61.csv','InpuScaling128/output_62.csv','InpuScaling128/output_63.csv','InpuScaling128/output_64.csv','InpuScaling128/output_65.csv','InpuScaling128/output_66.csv','InpuScaling128/output_67.csv','InpuScaling128/output_68.csv','InpuScaling128/output_69.csv','InpuScaling128/output_70.csv','InpuScaling128/output_71.csv','InpuScaling128/output_72.csv','InpuScaling128/output_73.csv','InpuScaling128/output_74.csv','InpuScaling128/output_75.csv','InpuScaling128/output_76.csv','InpuScaling128/output_77.csv','InpuScaling128/output_78.csv','InpuScaling128/output_79.csv','InpuScaling128/output_80.csv','InpuScaling128/output_81.csv','InpuScaling128/output_82.csv','InpuScaling128/output_83.csv','InpuScaling128/output_84.csv','InpuScaling128/output_85.csv','InpuScaling128/output_86.csv','InpuScaling128/output_87.csv','InpuScaling128/output_88.csv','InpuScaling128/output_89.csv','InpuScaling128/output_90.csv','InpuScaling128/output_91.csv','InpuScaling128/output_92.csv','InpuScaling128/output_93.csv','InpuScaling128/output_94.csv','InpuScaling128/output_95.csv','InpuScaling128/output_96.csv','InpuScaling128/output_97.csv','InpuScaling128/output_98.csv','InpuScaling128/output_99.csv','InpuScaling128/output_100.csv','InpuScaling128/output_101.csv','InpuScaling128/output_102.csv','InpuScaling128/output_103.csv','InpuScaling128/output_104.csv','InpuScaling128/output_105.csv','InpuScaling128/output_106.csv','InpuScaling128/output_107.csv','InpuScaling128/output_108.csv','InpuScaling128/output_109.csv','InpuScaling128/output_110.csv','InpuScaling128/output_111.csv','InpuScaling128/output_112.csv','InpuScaling128/output_113.csv','InpuScaling128/output_114.csv','InpuScaling128/output_115.csv','InpuScaling128/output_116.csv','InpuScaling128/output_117.csv','InpuScaling128/output_118.csv','InpuScaling128/output_119.csv','InpuScaling128/output_120.csv','InpuScaling128/output_121.csv','InpuScaling128/output_122.csv','InpuScaling128/output_123.csv','InpuScaling128/output_124.csv','InpuScaling128/output_125.csv','InpuScaling128/output_126.csv','InpuScaling128/output_127.csv','InpuScaling128/output_128.csv']
# futures = exec.map(newSortNumpy , input_data,s3_file_url=True)
# ,instance_specify="large",s3_file_url=True
futures2 = exec.reducer(AggNaja,futures,instance_specify="medium")
# pywren.wait(futures)
# wrenexec = pywren.default_executor()
# future = wrenexec.call_async(printEQ, 3)
# print("future here<<<<<<<<<<<<<<<<<<<<<<<")
# print(future)
# print("future here<<<<<<<<<<<<<<<<<<<<<<<")
a = futures2.result_state()

print(a)
