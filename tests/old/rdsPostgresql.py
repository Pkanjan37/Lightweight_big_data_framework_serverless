import psycopg2
import logging
import numpy as np
import sys
import time
import json
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('TkAgg')
import uuid
from datetime import datetime
# logging.warning('Watch out!')  # will print a message to the console
# logging.error('I told you so')  # will not print anything
# a ={'data': 0, 'call_id': '00000'}
import boto3
# Plot
import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
def initEngine():
#edit connection string here
    engine = psycopg2.connect(
   
    )
    return engine
def  selectTableAllOS_ORDER(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from OS_ORDER")
    record = cur.fetchall()
    print(record)
    return record
def  selectTableAllOS_ORDER_ITEM(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from OS_ORDER_ITEM")
    record = cur.fetchall()
    print(record)
    return record
def  selectEvaluation(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from OS_ORDER_ITEM INNER JOIN OS_ORDER on OS_ORDER_ITEM.ORDER_ID = OS_ORDER.ORDER_ID where OS_ORDER_ITEM.SHOP_PRICE > 10.00") 
    record = cur.fetchall()
    print(record)
    return record
def  selectEvaluationReducer(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from OS_ORDER_ITEM INNER JOIN OS_ORDER on OS_ORDER_ITEM.ORDER_ID = OS_ORDER.ORDER_ID where OS_ORDER_ITEM.SHOP_PRICE > 10.00") 
    record = cur.fetchall()
    print(record)
    return record
def createOS_ORDER_Join_ITEM(engine):
    cur = engine.cursor()
    try:
        cur.execute("CREATE TABLE OS_INTEMEDIATE as SELECT * FROM OS_ORDER_ITEM INNER JOIN OS_ORDER on OS_ORDER_ITEM.ORDER_ID = OS_ORDER.ORDER_ID;")
        # cur.execute("INSERT INTO test (num,data) values (5,'eiei')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)
    except:
        print("I can't drop our test database!")

    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def createOS_ORDER(engine):
    cur = engine.cursor()
    try:
        cur.execute("CREATE TABLE OS_ORDER ( ORDER_ID    INTEGER NOT NULL,ORDER_CODE     INTEGER NOT NULL,BUYER_ID     INTEGER NOT NULL,CREATE_DT  DATE NOT NULL,PAY_DT    DATE,CREATE_IP  VARCHAR,ORDER_STATUS    VARCHAR,EXCEPTION_STATUS         VARCHARNULL;")
        # cur.execute("INSERT INTO test (num,data) values (5,'eiei')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)
    except:
        print("I can't drop our test database!")

    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def createOS_ORDER_ITEM(engine):
    cur = engine.cursor()
    try:
        cur.execute("CREATE TABLE OS_ORDER_ITEM ( ITEM_ID    INTEGER NOT NULL,ORDER_ID     INTEGER NOT NULL,GOODS_ID     INTEGER NOT NULL,GOODS_NUMBER  DECIMAL(15,2),SHOP_PRICE    DECIMAL(15,6),GOODS_PRICE  DECIMAL(15,2),GOODS_AMOUNT    DECIMAL(15,6);")
        # cur.execute("INSERT INTO test (num,data) values (5,'eiei')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)
    except:
        print("I can't drop our test database!")

    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
# print(engine)
def createTableLineItem(engine):
    cur = engine.cursor()
    try:
        cur.execute("CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,L_PARTKEY     INTEGER NOT NULL,L_SUPPKEY     INTEGER NOT NULL,L_LINENUMBER  INTEGER NOT NULL,L_QUANTITY    DECIMAL(15,2) NOT NULL,L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,L_DISCOUNT    DECIMAL(15,2) NOT NULL,L_TAX         DECIMAL(15,2) NOT NULL,L_RETURNFLAG  CHAR(1) NOT NULL,L_LINESTATUS  CHAR(1) NOT NULL,L_SHIPDATE    DATE NOT NULL,L_COMMITDATE  DATE NOT NULL,L_RECEIPTDATE DATE NOT NULL,L_SHIPINSTRUCT CHAR(25) NOT NULL,L_SHIPMODE     CHAR(10) NOT NULL,L_COMMENT      VARCHAR(44) NOT NULL);")
        # cur.execute("INSERT INTO test (num,data) values (5,'eiei')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)
    except:
        print("I can't drop our test database!")

    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def selectColumn(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'LINEITEM';")
    record = cur.fetchall()
    print(record)
    return record
def  selectTableAllLineItem(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from LINEITEM")
    record = cur.fetchall()
    print(record)
    return record
def selectSchema(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("SELECT schema_name FROM information_schema.schemata")
    # cur.execute("select * from information_schema.tables")
    record = cur.fetchall()
    print(record)
    return record
def dropLineItem(engine):
    cur = engine.cursor()
    cur.execute('DROP TABLE "LINEITEM";')  
    engine.commit()
    engine.close()
def insertCSVLineItem(engine):
    # engine = create_engine('postgresql://root:032400144@thesis.ctcgc8i6drwr.eu-central-1.rds.amazonaws.com:5432/postgres')
    cnt=0
    cur = engine.cursor()

    colnames=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER','L_QUANTITY','L_EXTENDEDPRICE','L_DISCOUNT','L_TAX','L_RETURNFLAG','L_LINESTATUS','L_SHIPDATE','L_COMMITDATE','L_RECEIPTDATE','L_SHIPINSTRUCT','L_SHIPMODE','L_COMMENT'] 
    for df in pd.read_csv('/home/p/tpch-kit/dbgen/output/lineitem.csv',sep=',', header = None,chunksize=1):
        # print(df[9].to_string())
        # insertString="INSERT INTO LINEITEM (L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT) values ('"+df[0]+"','"+df[1]+"','"+df[2]+"','"+df[3]+"','"+df[4]+"','"+df[5]+"','"+df[6]+"','"+df[7]+"','"+df[8]+"','"+df[9]+"','"+df[10]+"','"+df[11]+"','"+df[12]+"','"+df[13]+"','"+df[14]+"','"+df[15]+"')"
        # df.to_sql('LINEITEM', engine, schema='public', index=False, if_exists='append'),{11},{12},{13},{14},{15}
        if(cnt!=0):
            insertString="INSERT INTO LINEITEM (L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT) values ({0},{1},{2},{3},{4:.2f},{5:.2f},{6:.2f},{7:.2f},'{8}','{9}', TIMESTAMP '{10}',TIMESTAMP '{11}',TIMESTAMP '{12}','{13}','{14}','{15}')".format(int(df[0]),int(df[1]),int(df[2]),int(df[3]),float(df[4]),float(df[5]),float(df[6]),float(df[7]),df[8].to_string(index=False).strip(),df[9].to_string(index=False).strip(), datetime.strptime(df[10].to_string(index=False).strip(), "%Y-%m-%d"),datetime.strptime(df[11].to_string(index=False).strip(), "%Y-%m-%d"),datetime.strptime(df[12].to_string(index=False).strip(), "%Y-%m-%d"),df[13].to_string(index=False).strip(),df[14].to_string(index=False).strip(),df[15].to_string(index=False).strip()) 
            print(insertString)
            cur.execute(insertString)   
        # if(cnt==1):
        # break
        cnt=cnt+1
def insertOS_INTERMEDIATE(engine):
    # engine = create_engine('postgresql://root:032400144@thesis.ctcgc8i6drwr.eu-central-1.rds.amazonaws.com:5432/postgres')
    cnt=0
    cur = engine.cursor()
      
    # for df in pd.read_csv('/media/p/Elements/downloads/ECT.tar/ECT/ECT/OS_ORDER.txt',sep='\t', chunksize=1,engine='python'):
        # print(df[9].to_string())
        # insertString="INSERT INTO LINEITEM (L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT) values ('"+df[0]+"','"+df[1]+"','"+df[2]+"','"+df[3]+"','"+df[4]+"','"+df[5]+"','"+df[6]+"','"+df[7]+"','"+df[8]+"','"+df[9]+"','"+df[10]+"','"+df[11]+"','"+df[12]+"','"+df[13]+"','"+df[14]+"','"+df[15]+"')"
        # df.to_sql('LINEITEM', engine, schema='public', index=False, if_exists='append'),{11},{12},{13},{14},{15}
    insertString="INSERT INTO OS_INTERMEDIATE (ORDER_ID,ORDER_CODE,BUYER_ID,CREATE_DT,PAY_DT,CREATE_IP,ORDER_STATUS,EXCEPTION_STATUS,ITEM_ID,GOODS_ID,GOODS_NUMBER,SHOP_PRICE,GOODS_PRICE,GOODS_AMOUNT) select"  
    print(insertString)
    cur.execute(insertString)   
    # if(cnt==1):
        # break
    engine.commit() # <--- makes sure the change is shown in the database
    print("complete")
    engine.close()
    cur.close()

def insertOS_ORDER(engine):
    # engine = create_engine('postgresql://root:032400144@thesis.ctcgc8i6drwr.eu-central-1.rds.amazonaws.com:5432/postgres')
    cnt=0
    cur = engine.cursor()
      
    for df in pd.read_csv('/media/p/Elements/downloads/ECT.tar/ECT/ECT/OS_ORDER.txt',sep='\t', chunksize=1,engine='python'):
        # print(df[9].to_string())
        # insertString="INSERT INTO LINEITEM (L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT) values ('"+df[0]+"','"+df[1]+"','"+df[2]+"','"+df[3]+"','"+df[4]+"','"+df[5]+"','"+df[6]+"','"+df[7]+"','"+df[8]+"','"+df[9]+"','"+df[10]+"','"+df[11]+"','"+df[12]+"','"+df[13]+"','"+df[14]+"','"+df[15]+"')"
        # df.to_sql('LINEITEM', engine, schema='public', index=False, if_exists='append'),{11},{12},{13},{14},{15}
        insertString="INSERT INTO OS_ORDER (ORDER_ID,ORDER_CODE,BUYER_ID,CREATE_DT,PAY_DT,CREATE_IP,ORDER_STATUS,EXCEPTION_STATUS) values ({0},{1},{2},TIMESTAMP '{3}',TIMESTAMP '{4}',{5},{6},{7}')".format(int(df.iloc[0][df.columns[0]]),int(df.iloc[0][df.columns[1]]),int(df.iloc[0][df.columns[2]]),datetime.strptime(df.iloc[0][df.columns[3]].to_string(index=False).strip(), "%Y-%m-%d"),datetime.strptime(df.iloc[0][df.columns[4]].to_string(index=False).strip(), "%Y-%m-%d"),df.iloc[0][df.columns[5]].to_string(index=False).strip(),df.iloc[0][df.columns[6]].to_string(index=False).strip(),df.iloc[0][df.columns[7]].to_string(index=False).strip()) 
        print(insertString)
        cur.execute(insertString)   
        # if(cnt==1):
        # break
    engine.commit() # <--- makes sure the change is shown in the database
    print("complete")
    engine.close()
    cur.close()

def insertOS_ORDER_ITEMs(engine):
    # engine = create_engine('postgresql://root:032400144@thesis.ctcgc8i6drwr.eu-central-1.rds.amazonaws.com:5432/postgres')
    cnt=0
    cur = engine.cursor()
      
    for df in pd.read_csv('/media/p/Elements/downloads/ECT.tar/ECT/ECT/OS_ORDER_ITEMS.txt',sep='\t', chunksize=1,engine='python'):
        # print(df[9].to_string())
        # insertString="INSERT INTO LINEITEM (L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT) values ('"+df[0]+"','"+df[1]+"','"+df[2]+"','"+df[3]+"','"+df[4]+"','"+df[5]+"','"+df[6]+"','"+df[7]+"','"+df[8]+"','"+df[9]+"','"+df[10]+"','"+df[11]+"','"+df[12]+"','"+df[13]+"','"+df[14]+"','"+df[15]+"')"
        # df.to_sql('LINEITEM', engine, schema='public', index=False, if_exists='append'),{11},{12},{13},{14},{15}
        insertString="INSERT INTO OS_ORDER_ITEM (ITEM_ID,ORDER_ID,GOODS_ID,GOODS_NUMBER,SHOP_PRICE,GOODS_PRICE,GOODS_AMOUNT) values ({0},{1},{2},{3:.2f},{4:.6f},{5:.2f},{6:.6f}')".format(int(df.iloc[0][df.columns[0]]),int(df.iloc[0][df.columns[1]]),int(df.iloc[0][df.columns[2]]),float(df.iloc[0][df.columns[3]]),float(df.iloc[0][df.columns[4]]),float(df.iloc[0][df.columns[5]]),float(df.iloc[0][df.columns[6]])) 
        print(insertString)
        cur.execute(insertString)   
        # if(cnt==1):
        # break
    engine.commit() # <--- makes sure the change is shown in the database
    print("complete")
    engine.close()
    cur.close()
def insertCSVOrder(engine):
    # engine = create_engine('postgresql://root:032400144@thesis.ctcgc8i6drwr.eu-central-1.rds.amazonaws.com:5432/postgres')
    cnt=0
    cur = engine.cursor()

    # colnames=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER','L_QUANTITY','L_EXTENDEDPRICE','L_DISCOUNT','L_TAX','L_RETURNFLAG','L_LINESTATUS','L_SHIPDATE','L_COMMITDATE','L_RECEIPTDATE','L_SHIPINSTRUCT','L_SHIPMODE','L_COMMENT'] 
    for df in pd.read_csv('/home/p/tpch-kit/dbgen/output/orders.csv',sep=',', header = None,chunksize=1):
        # print(df[9].to_string())
        # insertString="INSERT INTO LINEITEM (L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT) values ('"+df[0]+"','"+df[1]+"','"+df[2]+"','"+df[3]+"','"+df[4]+"','"+df[5]+"','"+df[6]+"','"+df[7]+"','"+df[8]+"','"+df[9]+"','"+df[10]+"','"+df[11]+"','"+df[12]+"','"+df[13]+"','"+df[14]+"','"+df[15]+"')"
        # df.to_sql('LINEITEM', engine, schema='public', index=False, if_exists='append'),{11},{12},{13},{14},{15}
        # if(cnt!=0):
        insertString="INSERT INTO ORDERS (O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT) values ({0},{1},'{2}',{3:.2f},TIMESTAMP '{4}','{5}','{6}',{7},'{8}')".format(int(df[0]),int(df[1]),df[2].to_string(index=False).strip(),float(df[3]),datetime.strptime(df[4].to_string(index=False).strip(), "%Y-%m-%d"),df[5].to_string(index=False).strip(),df[6].to_string(index=False).strip(),int(df[7]),df[8].to_string(index=False).strip()) 
        print(insertString)
        cur.execute(insertString)   
        # if(cnt==1):
        # break
        # cnt=cnt+1
    # cur = engine.cursor()
    # # cur.execute("COPY table_name FROM '/home/p/tpch-kit/dbgen/output/lineitem.csv' DELIMITERS ',' CSV;")
    # cur.execute("INSERT INTO ResultReserve (WorkerTime,SessionId,WorkerARN) values ('"+str(workTime)+"','"+str(SessionId)+"','"+str(i['executionArn'])+"')")   
    engine.commit() # <--- makes sure the change is shown in the database
    print("complete")
    engine.close()
    cur.close()
def createTableLineOrder(engine):
    cur = engine.cursor()
    try:
        cur.execute("CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,O_CUSTKEY        INTEGER NOT NULL,O_ORDERSTATUS    CHAR(1) NOT NULL,O_TOTALPRICE     DECIMAL(15,2) NOT NULL,O_ORDERDATE      DATE NOT NULL,O_ORDERPRIORITY  CHAR(15) NOT NULL,  O_CLERK          CHAR(15) NOT NULL, O_SHIPPRIORITY   INTEGER NOT NULL,O_COMMENT        VARCHAR(79) NOT NULL);")
        # cur.execute("INSERT INTO test (num,data) values (5,'eiei')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)
    except:
        print("I can't drop our test database!")

    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def  selectTableAllOrder(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from ORDERS")
    record = cur.fetchall()
    print(record)
    return record
def queryOne(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,avg(l_quantity) as avg_qty,avg(l_extendedprice) as avg_price,avg(l_discount) as avg_disc,count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval ':1' day (3) group by l_returnflag, l_linestatus order by l_returnflag,l_linestatus;")
    record = cur.fetchall()
    print(record)
    return record

def createTable(engine):
    cur = engine.cursor()
    try:
        cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId varchar);")
        # cur.execute("INSERT INTO test (num,data) values (5,'eiei')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)
    except:
        print("I can't drop our test database!")

    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def createTable2(engine):
    cur = engine.cursor()
    try:
        cur.execute("CREATE TABLE ResultLogger (id serial PRIMARY KEY, FuncTime varchar,JobTime varchar,SessionId varchar);")
        # cur.execute("INSERT INTO test (num,data) values (5,'eiei')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)
    except:
        print("I can't drop our test database!")

    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def createTable3(engine):
    cur = engine.cursor()
    try:
        cur.execute("CREATE TABLE ResultGraph (id serial PRIMARY KEY, FuncTime varchar,JobTime varchar,SessionId varchar, GraphSet varchar);")
        # cur.execute("INSERT INTO test (num,data) values (5,'eiei')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)
    except:
        print("I can't drop our test database!")

    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def createTable4(engine):
    cur = engine.cursor()
    try:
        cur.execute("CREATE TABLE ResultReserve (id serial PRIMARY KEY, WorkerTime varchar,SessionId varchar, WorkerARN varchar);")
        # cur.execute("INSERT INTO test (num,data) values (5,'eiei')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)
    except:
        print("I can't drop our test database!")

    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def  insertTable(engine,UUID,TimeStamp,EventName,SessionId,StateId):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
    cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values ('"+str(UUID)+"','"+str(TimeStamp)+"','"+EventName+"','"+str(SessionId)+"','"+StateId+"')")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)

    engine.commit() # <--- makes sure the change is shown in the database
    print("complete")
    # engine.close()
    cur.close()

def  selectTableAll(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from Logger")
    record = cur.fetchall()
    print(record)
    return record

def  selectTableAll2(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from ResultLogger")
    record = cur.fetchall()
    print(record)
    return record


    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def  selectTableAll3(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from ResultGraph")
    record = cur.fetchall()
    print(record)
    return record


    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()
def  selectTableAll4(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
    cur.execute("select * from ResultReserve")
    record = cur.fetchall()
    print(record)
    return record


    engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
    cur.close()

def  selectTable(engine,SessionId):
        cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
        cur.execute("select * from Logger where SessionId ='"+SessionId+"'")
        record = cur.fetchall()
        print(record)
        return record

        engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
        cur.close()
def  selectTable2(engine,SessionId):
        cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
        cur.execute("select * from ResultLogger where SessionId ='"+SessionId+"'")
        record = cur.fetchall()
        print(record)
        return record

        engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
        cur.close()
def  selectTable3(engine,GraphSet):
        cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
        cur.execute("select * from ResultGraph where GraphSet ='"+GraphSet+"'")
        record = cur.fetchall()
        print(record)
        return record

        engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
        cur.close()

def  selectTable3point5(engine,SessionId):
        cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
        cur.execute("select * from ResultGraph where SessionId ='"+SessionId+"'")
        record = cur.fetchall()
        print(record)
        return record

        engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
        cur.close()
def deleteTable3(engine,SessionId):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
    cur.execute("DELETE FROM ResultGraph WHERE SessionId='"+SessionId+"'")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)

    engine.commit() # <--- makes sure the change is shown in the database
    print("complete")
    # engine.close()
    cur.close()

def deleteTable3All(engine):
    cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
    cur.execute("DELETE FROM ResultGraph")
        # cur.execute("select * from test")
        # record = cur.fetchone()
        # print(record)

    engine.commit() # <--- makes sure the change is shown in the database
    print("complete")
    # engine.close()
    cur.close()

def  selectTable4(engine,WorkerARN):
        cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
        cur.execute("select * from ResultReserve where WorkerARN = '"+WorkerARN+"'" )
        record = cur.fetchall()
        print(record)
        return record

        engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
        cur.close()
def  selectTable4point5(engine,SessionId):
        cur = engine.cursor()
        # cur.execute("CREATE TABLE Logger (id serial PRIMARY KEY, UUID varchar, TimeStamp varchar,EventName varchar,SessionId varchar,StateId integer);")
        # cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values (5,'eiei')")
        cur.execute("select * from ResultReserve where SessionId = '"+SessionId+"'" )
        record = cur.fetchall()
        print(record)
        return record

        engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
        cur.close()
def insertDBStepFuncLog(engine,SessionId,arn):
    cur = engine.cursor()
    client = boto3.client('stepfunctions')
    response = client.list_executions(
    stateMachineArn=arn,
    maxResults=123
    )
# print(response)
    for i in response['executions']:
        response = client.get_execution_history(
            executionArn=i['executionArn'],
            maxResults=1000
        )
        temp = []
        for t in response['events']:
            a = t['timestamp']
            timestamp = {'timestamp':a.timestamp()}
            typeExecution = {'type': t['type']}
            id = {'id':t['id']}
            pre = {'previousEventId':t['previousEventId']}
            UUID = uuid.uuid1().hex
            cur.execute("INSERT INTO Logger (UUID,TimeStamp,EventName,SessionId,StateId) values ('"+str(UUID)+"','"+str(a.timestamp())+"','"+str(t['type'])+"','"+str(SessionId)+"','"+str(t['id'])+"')")
            if t['type'] == "LambdaFunctionScheduled":
                start =   a.timestamp()
            if t['type'] == "LambdaFunctionSucceeded":
                end = a.timestamp()
            engine.commit() # <--- makes sure the change is shown in the database
    # engine.close()
            # cur.close()
            all = {}
            all.update(typeExecution)
            all.update(timestamp)
            
            all.update(id)
            all.update(pre)
            temp.append(all)
            print(all)
            print("<<<<<<<<<<<<<")
        workTime = end - start
        print("Time consume for worker :",i['executionArn'] )
        print(workTime)

        cur.execute("INSERT INTO ResultReserve (WorkerTime,SessionId,WorkerARN) values ('"+str(workTime)+"','"+str(SessionId)+"','"+str(i['executionArn'])+"')")   
        # print(response)
        engine.commit()
        result = json.dumps(temp)
        # print(result)
        data = json.loads(result)
        # print(data)
        # print("--------------------")
    
        df = pd.DataFrame.from_dict(data, orient='columns')
        ax = df.plot.bar(rot=0)
        
        # print(df["timestamp"])
        # print("------------------------------------")

        # print(response)
def InsertResultplotter(engine,SessionId,GraphSet):
    record = selectTable(engine,SessionId)
    cur = engine.cursor()
    haveFuncStart= False
    haveFuncComplete = False
    SumFuncCompute =0
    for i in record:
        print(i[3])
        if( i[3]=="LightWeightStartExecution"):
            start = i[2]
            print("StartTime = "+start)
        if(i[3] == "LightWeightCompleteExecution"):
            end = i[2]
            print("EndtTime = "+end)
        if(i[3]== "LambdaFunctionScheduled"):
            funcStart = i[2]
            print("FuncStartTime = "+funcStart)
            haveFuncStart = True
        if(i[3]=="LambdaFunctionSucceeded"):   
            funcEnd = i[2]
            print("FuncEndtTime = "+funcEnd)
            haveFuncComplete = True
        if haveFuncComplete==True and haveFuncStart == True:
            SumFuncCompute = SumFuncCompute + float(funcEnd)-float(funcStart)
            print("SumFuncCompute : ",SumFuncCompute)
            funcStart = 0
            funcEnd = 0
            haveFuncComplete = False
            haveFuncStart = False

        
    # jobsTime = end -start
    # jobsTime = "None"
    jobsTime = float(end)-float(start)
    # jobsTime = "None"
    print("jobsTime = ",jobsTime)
    # funcTime = funcEnd - funcStart
    # funcTime = (float(funcEnd)-float(funcStart))/(60*60*24)
    print("funcTime = ",SumFuncCompute)
    # cur.execute("INSERT INTO ResultLogger (FuncTime,JobTime,SessionId) values ('"+str(funcTime)+"','"+str(jobsTime)+"','"+SessionId+"');")
    cur.execute("INSERT INTO ResultGraph (FuncTime,JobTime,SessionId,GraphSet) values ('"+str(SumFuncCompute)+"','"+str(jobsTime)+"','"+SessionId+"','"+GraphSet+"');")
    # cur.execute("UPDATE ResultGraph SET FuncTime='"+str(SumFuncCompute)+"', JobTime='"+str(jobsTime)+"' WHERE SessionId = '"+SessionId+"' ;")
    engine.commit()
    # plt.plot([1,5,3,4])
    # plt.ylabel('some numbers')
    # plt.xlabel('some numbers2')
    # plt.show()

def HarryPlotter(engine,GraphSet):
    record = selectTable3(engine,GraphSet)
    firstGraph = []
    secondGraph= []
    # print(record)
    for i in record:
        firstGraph.append(i[2])
        secondGraph.append(i[1])
    # firstGraph.sort()
    # secondGraph.sort()
    print("First <<<<<<<<")
    print(firstGraph)
    print("Second <<<<<<<")
    print(secondGraph)
    # plt.figure(1)
    # plt.subplot(211)
    # plt.plot(firstGraph)
    

    # plt.subplot(212)
    # plt.plot(secondGraph)
    # for i in range(0,len(secondGraph)):
    #     secondGraph[i]=float(secondGraph[i])
    #     print(secondGraph[i])
    # x = secondGraph
    # y = [1,2,3,4,5]
    # plt.ylabel('Total time consume')
    # plt.xlabel('Round')
    # plt.title("Time consuming for complete the 3.2GB sorting task with 128 worker")
    # plt.scatter(y, x)
    # plt.show()
    # plt.close('all')
def graphCreating():
    # df=pd.DataFrame({'x': range(1,11), 'y1': np.random.randn(10), 'y2': np.random.randn(10)+range(1,11), 'y3': np.random.randn(10)+range(11,21) })
    # multiple line plot
    # plt.plot( 'x', 'y1', data=df, marker='o', markerfacecolor='blue', markersize=12, color='skyblue', linewidth=4)
    # plt.plot( 'x', 'y2', data=df, marker='', color='olive', linewidth=2)
    # plt.plot( 'x', 'y3', data=df, marker='', color='olive', linewidth=2, linestyle='dashed', label="toto")
    # plt.legend()
    fig = plt.figure()
    ax = plt.axes(projection='3d')
    # Data for a three-dimensional line
    zline = np.linspace(0, 15, 1000)
    xline = np.sin(zline)
    yline = np.cos(zline)
    ax.plot3D(xline, yline, zline, 'gray')

    # Data for three-dimensional scattered points
    # zdata = 15 * np.random.random(100)
    # xdata = np.sin(zdata) + 0.1 * np.random.randn(100)
    # ydata = np.cos(zdata) + 0.1 * np.random.randn(100)
    # ax.scatter3D(xdata, ydata, zdata, c=zdata, cmap='Greens')

def costCalculator(ListTimeTake,size,p,g):
    if size == "small":
        instance_cost = 2.08*(10**(-10))
    elif size == "medium":
        instance_cost = 2.605*(10**(-9))
    else:
        instance_cost = 4.367*(10**(-9))
    LambdaCost = 0
    numberOfWorker = len(ListTimeTake)
    for i in ListTimeTake:
        LambdaCost = LambdaCost+((i*instance_cost)+0.2/10**6)
    print("Lambda cost :",LambdaCost)
    SfnCost =2.5*(10**(-5))*numberOfWorker
    print("Step function cost :",SfnCost)
    S3Cost = (10**(-5))*(0.54*p+0.043*g)
    print("S3 cost :",S3Cost)
    print("Sum :",S3Cost+SfnCost+LambdaCost)
    


engine = initEngine()
# uuid1 = uuid.uuid1().hex
Complexity = "1"
Fibo = "2"
inputScaling = "3"
workerScaling="4"
workerOnlyScaling="5"
fluctuate="6"
workScalingFix="7"
whySoFast="8"
VsPyWren = "9"
SQL="10"
# insertCSVOrder(engine)
# selectTableAllOrder(engine)
# queryOne(engine)
# selectSchema(engine)
# selectColumn(engine)
# selectTableAllLineItem(engine)
# dropLineItem(engine)
# insertCSVLineItem(engine)
# createTableLineOrder(engine)
# selectTableAllOrder(engine)
# costCalculator([42.3050000667572],"large",2,2)
# createTable(engine)
# createTable2(engine)
# createTable3(engine)
# createTable4(engine)

# selectTableAll(engine)
# selectTableAll2(engine)
# selectTableAll3(engine)
# selectTableAll4(engine)
selectTable(engine,"1d5ab761-2413-3c78-bae5-fae3bd725beb")
# selectTable3(engine,"5")
# selectTable3point5(engine,"a22cbe05-d1fc-3dd2-9521-0f9f1ad0f9d9")
# selectTable4(engine,"arn:aws:states:eu-central-1:251584899486:execution:On-1561415719.9803038:421d2398-1ca3-45ac-a8de-4a18249ab94f")
# selectTable4point5(engine,"TEST NAJA")
# insertDBStepFuncLog(engine,"TEST NAJA","arn:aws:states:eu-central-1:251584899486:stateMachine:FilterNaja-1565567452.4555542")
# InsertResultplotter(engine,SQL,SQL)
#51a25f05-c2e4-3bb0-bc8e-ce1576724254---128 worker
#280a4fd7-6a70-3ad2-97b4-0ae0e6f24ec2=32worker

# HarryPlotter(engine,SQL)

# deleteTable3(engine,"8083ce1f-aef0-3e65-a9c3-d240b1e34621")
# deleteTable3All(engine)
