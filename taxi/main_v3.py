# -*- coding: utf-8 -*-
import math
import os
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType,udf
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import *
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

"""

input dir format like:  ./data/2014-07-01

"""
INPUT_DIR="../../data/taxi"
JQBH_encode_csv_dir="../../data/taxi/JQBH_encode.csv"

"""
above could be changed

"""
if __name__ == "__main__":
    print("======")
    # 配置环境
    # 设置默认分成多少个区
    spark = SparkSession.builder.appName("taSpark").config("spark.default.parallelism", 20).getOrCreate()

    print("start")
    #root
    # root="./data"
    root=INPUT_DIR
    rootdir=os.listdir(root)
    rootdir=[root+"/"+i for i in rootdir if "DS_Store" not in i ]

    # JQBH进行编码
    jqbh_code=pd.read_csv(JQBH_encode_csv_dir)
    jqbh_code=dict(zip(jqbh_code["name"].values.tolist(), jqbh_code["code"].values.tolist()))

    for day in rootdir:

        a=[]
        for i in [day]:
            temp=os.listdir(i)
            temp=[i+"/"+j for j in temp]
            a=a+temp
            # break
            pass
        filepath=a



        data = spark.read.csv(filepath,sep='\t', header=True,encoding='GB18030', inferSchema = "true")
        data.createOrReplaceTempView("data_sql")

        data=data.filter(data.JD > 117.95)
        data = data.filter(data.JD < 118.24)
        data=data.filter(data.WD > 24.42)
        data = data.filter(data.WD < 24.54)

        #处理JQBH



        # pandas_udf("integer", PandasUDFType.SCALAR)
        def fc(a):
            try:
                return jqbh_code[a]
            except:
                return -1
            pass

        fc=udf(fc, StringType())
        # data.select(fc('JQBH')).show()
        data=data.withColumn('JQBH',fc('JQBH'))
        data.createOrReplaceTempView("data_sql")


        #处理时间格式
        #jssj
        def proceesTime(x):
            month=x.split("-")[1].split("月")[0]
            day=x.split("-")[0]
            hhmmss=x.split(" ")[-1]
            return "2014-"+month+"-"+day+" "+hhmmss
        proceesTime = udf(proceesTime, StringType())

        def proceesMonth(x):
            month=x.split("-")[1].split("月")[0]
            return month
        proceesMonth = udf(proceesMonth, StringType())

        def proceesDay(x):
            day=x.split("-")[0]
            return day
        proceesDay = udf(proceesDay, StringType())

        def proceesHour(x):
            hour=x.split(" ")[-1].split(":")[0]
            return hour
        proceesHour = udf(proceesHour, StringType())


        def proceesMine(x):
            mine = x.split(" ")[-1].split(":")[1]
            return mine
        proceesMine = udf(proceesMine, StringType())

        def proceesSecond(x):
            second = x.split(" ")[-1].split(":")[2]
            return second
        proceesSecond = udf(proceesSecond, StringType())

        data=data.withColumn('JSSJ_month', proceesMonth('JSSJ'))
        data = data.withColumn('JSSJ_day', proceesDay('JSSJ'))
        data = data.withColumn('JSSJ_hour', proceesHour('JSSJ'))
        data = data.withColumn('JSSJ_mine', proceesMine('JSSJ'))
        data = data.withColumn('JSSJ_second', proceesSecond('JSSJ'))
        data = data.withColumn('JSSJ', proceesTime('JSSJ'))
        #GPSTIME
        data=data.withColumn('GPSTIME_month', proceesMonth('GPSTIME'))
        data = data.withColumn('GPSTIME_day', proceesDay('GPSTIME'))
        data = data.withColumn('GPSTIME_hour', proceesHour('GPSTIME'))
        data = data.withColumn('GPSTIME_mine', proceesMine('GPSTIME'))
        data = data.withColumn('GPSTIME_second', proceesSecond('GPSTIME'))
        data = data.withColumn('GPSTIME', proceesTime('GPSTIME'))

        data.createOrReplaceTempView("data_sql")

        data=spark.sql("select XXBH,JSSJ,JSSJ_month,JSSJ_day,JSSJ_hour,JSSJ_mine,JSSJ_second,\
        CLBH,JQBH,CAST(JD as double ) as JD,CAST(WD as double) as WD,CAST(GD as Integer) as GD,\
        GPSTIME,GPSTIME_month,GPSTIME_day,GPSTIME_hour,GPSTIME_mine,GPSTIME_second,\
        SUDU from data_sql ")
        data.createOrReplaceTempView("data_sql")

        #处理经纬度
        JD_max=118.24#data.agg({"JD": "max"}).collect()[0][0]
        JD_min = 117.95#data.agg({"JD": "min"}).collect()[0][0]
        JDlist=np.linspace(JD_min, JD_max, 31, endpoint=True)
        JDlc_size=JDlist[1]-JDlist[0]
        JDlist=[str(i)[:7] for i in JDlist]


        WD_max=24.54#data.agg({"WD": "max"}).collect()[0][0]
        WD_min = 24.42 #data.agg({"WD": "min"}).collect()[0][0]
        WDlist=np.linspace(WD_min, WD_max, 31, endpoint=True)[::-1]
        WDlc_size=WDlist[1]-WDlist[0]
        WDlist = [str(i)[:6] for i in WDlist]

        def JD_location(x):
            global  JD_min
            global  JD_max
            global  JDlist
            global  JDlc_size

            if x== JD_min:
                DOWN=JDlist[0]
                UP=JDlist[1]
            elif x==JD_max:
                DOWN = JDlist[-2]
                UP = JDlist[-1]
            else:
                DOWN=JDlist[np.int((x-JD_min)/JDlc_size)]
                UP=JDlist[np.int((x-JD_min)/JDlc_size)+1]
            return str(DOWN)+"-"+str(UP)
        JD_location = udf(JD_location, StringType())
        data=data.withColumn('JD_LOC', JD_location('JD'))

        def WD_location(x):
            global  WD_min
            global  WD_max
            global  WDlist
            global  WDlc_size

            if x== WD_min:
                DOWN=WDlist[0]
                UP=WDlist[1]
            elif x==WD_max:
                DOWN = WDlist[-2]
                UP = WDlist[-1]
            else:
                DOWN=WDlist[np.int((x-WD_min)/WDlc_size)]
                UP=WDlist[np.int((x-WD_min)/WDlc_size)+1]
            return str(DOWN)+"-"+str(UP)
        WD_location = udf(WD_location, StringType())
        data = data.withColumn('WD_LOC', WD_location('WD'))
        data.createOrReplaceTempView("data_sql")

        JDname=[]
        for i,j in enumerate(JDlist):
            if i >=len(JDlist)-1:
                break
            JDname.append(str(JDlist[i])+"-"+str(JDlist[i+1]))
            pass

        WDname=[]
        for i,j in enumerate(WDlist):
            if i >=len(WDlist)-1:
                break
            WDname.append(str(WDlist[i])+"-"+str(WDlist[i+1]))
            pass

        # # day=spark.sql("select distinct(JSSJ_day) from data_sql").collect()
        # # day=[i[0] for i in day ]
        hour = spark.sql("select distinct(GPSTIME_hour) from data_sql").collect()
        hour = [i[0] for i in hour]

        print("182")

        # for i in day:
        for j in hour:
            print(j)
            showdata = [[0] * (len(JDname))] * (len(WDname))
            showdata = pd.DataFrame(showdata)
            showdata.columns = JDname
            showdata.index = WDname

            sql="select JD_LOC,WD_LOC,count(1) from data_sql where  GPSTIME_hour=%s group by JD_LOC,WD_LOC"%(j)
            a=spark.sql(sql).toPandas()
            print(a.shape)
            print("start")
            #将a转换成df
            # for ii in a['JD_LOC']:
            #     # print(ii)
            #     for jj in a["WD_LOC"]:
            #         try:
            #             showdata.loc[jj,ii]=a[(a['JD_LOC']==ii) & (a["WD_LOC"]==jj)].iloc[:,-1].values[0]
            #         except:
            #             pass
            #         # break
            #         pass
            #     # break
            #     pass
            def ff(x):
                global showdata
                showdata.loc[x[1], x[0]] = x[2]
                pass
            a.apply(ff, axis = 1)
            # 使用seaborn画图而不是plt
            sns.set()
            path="%s-%s.png"%(day.split("/")[-1],j)
            path_add_folder="%s/%s-%s.png"%('-'.join(path.split("-")[:3]),day.split("/")[-1],j)
            plt.figure(figsize=(20, 20))
            plt.title(j)
            sns.heatmap(data=showdata, xticklabels=True, linewidths=0.1, annot=True, mask=(showdata < 1),
                        annot_kws={'size': 9}, fmt='.20g')
            plt.savefig(path_add_folder)
            plt.show()
            # break
            pass
        #merge and  write by one day (if not need you can comment it
        data.repartition(1).write.csv('-'.join(path.split("-")[:3]),mode="overwrite",header=True)
        break
        pass


