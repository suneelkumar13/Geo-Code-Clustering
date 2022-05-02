import json
import sys
from pyspark import SparkContext,SparkConf
import numpy as np
import pandas as pd

from math import sin, cos, sqrt, radians, atan2, degrees, asin
from io import StringIO

from pyspark.sql.types import *
from pyspark.sql.functions import split
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

import re
import os
import sys
import time

#finds euclidian distance
def calc_euclidian_dist(p1_lat,p1_long,p2_lat,p2_long):
    #Initializing radius of earth in KM
    radius_of_earth=6371
    #print(p1_lat,"-",p1_long,"-",p2_lat,"-",p2_long)
    
    #Converding to radians
    p1_lat_c=radians(float(p1_lat))
    p1_long_c=radians(float(p1_long))
    p2_lat_c=radians(float(p2_lat))
    p2_long_c=radians(float(p2_long))

    #Finding cartesian coordinates
    x1=radius_of_earth*cos(p1_lat_c)*cos(p1_long_c)
    y1=radius_of_earth*cos(p1_lat_c)*sin(p1_long_c)
    z1=radius_of_earth*sin(p1_lat_c)

    x2=radius_of_earth*cos(p2_lat_c)*cos(p2_long_c)
    y2=radius_of_earth*cos(p2_lat_c)*sin(p2_long_c)
    z2=radius_of_earth*sin(p2_lat_c)

    euclidian_dist=sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)
    return euclidian_dist


#finds mean of the points
def add_points(centroid):
    centroid_lat=[]
    centroid_long=[]
    for lat in centroid[0]:
        centroid_lat.append(float(lat))
    for long in centroid[1]:
        centroid_long.append(float(long))
    avg_lat_deg=sum(centroid_lat)/len(centroid_lat)
    avg_long_deg=sum(centroid_long)/len(centroid_long)
    return [avg_lat_deg,avg_long_deg]

#finds closest centroid to each point
def closest_centroid(point):
    x=[]
    intermediate=point[0].split(" ")
    intermediate_float=[float(x) for x in intermediate]
    for i in range(0,2*6,2):
        x.append(calc_euclidian_dist(intermediate_float[i],intermediate_float[i+1],float(point[1]),float(point[2])))
    return x.index(min(x))

#user defined pyspark functions
cc = F.udf(lambda point: closest_centroid(point), IntegerType())
distance_old_new = F.udf(lambda point: calc_euclidian_dist(point[0],point[1],point[2],point[3]),FloatType())
add_dd = F.udf(lambda centroid: add_points(centroid), ArrayType(FloatType()))


def main(sc,SQLContext,inp,out):
    
    carDataRaw = sc.textFile(inp)
    cars=carDataRaw.map(lambda line: line.split(",")).\
    map(lambda line: [line[0],line[1]]).\
    filter(lambda line: re.match("-?\d+\.\d+", line[0])).\
    filter(lambda line: re.match("-?\d+\.\d+", line[1]))

    print(type(cars))

    #initializations
    k=6
    lat=[]
    longi=[]
    ws=Window.orderBy(F.lit(1))
    
    data_df=cars.toDF() 
    y=data_df.count()

    #taking initial centroids of size k
    centroids=data_df.rdd.takeSample(False, k,seed=1)
    for i in range(0,k):
        lat.append(centroids[i][0])
        longi.append(centroids[i][1])
    init_lat=pd.DataFrame(lat,columns=["latitude"])
    init_long=pd.DataFrame(longi,columns=["longitude"])
    init = pd.concat([init_lat, init_long], axis=1)
    
    print(init) 

    iternationDistanceFloat=99999999
    while iternationDistanceFloat>1:
    
        #preprocessing to convert datatypes for calculations and renaming columns
        centroid_df=pd.DataFrame(centroids)
        falttened_df=centroid_df.values.flatten()
        listToStr = ' '.join([str(elem) for elem in falttened_df])
        data_df2=data_df.withColumn('latitude',data_df['_1'].cast(StringType())).withColumn('longitude',data_df['_2'].cast(StringType())) 
        data_df3=data_df.withColumn('centroid',F.lit(listToStr))
        data_df3 = data_df3.withColumnRenamed("_1","latitude").withColumnRenamed("_2","longitude")
        
        #calling closest centroid UDF
        data_df_final=data_df3.withColumn("closest_centroid",cc(F.array('centroid','latitude', 'longitude')))
    
        #finding clusters
        data_df_final_list=data_df_final.groupby(data_df_final['closest_centroid'])\
        .agg(F.collect_list(data_df_final['latitude']),F.collect_list(data_df_final['longitude']))\
        .withColumnRenamed("collect_list(latitude)","lat_list",)\
        .withColumnRenamed("collect_list(longitude)","long_list")\
        .sort(data_df_final['closest_centroid'])
    
        #Finding mean of every cluster & converting to pandas df
        with_mean=data_df_final_list.withColumn("mean",add_dd(F.array('lat_list', 'long_list')))   
        new_centroids=with_mean.withColumn("latitude",with_mean.mean[0]).withColumn("longitude",with_mean.mean[1])
        new_centroid_mean=new_centroids.select("latitude","longitude")
        new_centroids_pd=new_centroid_mean.toPandas() 
    
        #replacing initial clusters with new centroids
        init=new_centroids_pd
        new_centroids=new_centroid_mean.withColumnRenamed("latitude","_1").withColumnRenamed("longitude","_2").collect()
        old_cent=SQLContext.createDataFrame(centroids)
        old_cent=old_cent.withColumnRenamed("_1","old_latitude").withColumnRenamed("_2","old_longitude").\
        withColumn("id",F.row_number().over(ws))
        new_centroid_mean=new_centroid_mean.withColumnRenamed("latitude","new_latitude").withColumnRenamed("longitude","new_longitude").\
        withColumn("id",F.row_number().over(ws))
        combined_old_new=old_cent.join(new_centroid_mean,on="id",how="outer")                              
    
        #calculating overall distance bewteen old and new clusters to find stop condition
        distance=combined_old_new.withColumn("distance",distance_old_new\
                (F.array("old_latitude","old_longitude","new_latitude","new_longitude"))).groupBy().agg(F.sum('distance'))
        iterationDistance=distance.collect()[0]
        iternationDistanceFloat=(float(iterationDistance[0]))
    
        #Assigning for next iteration
        centroids=new_centroids

    sc.parallelize(data_df_final_list.rdd.collect()).saveAsTextFile(out)

if __name__ == "__main__":
    conf = SparkConf().setAppName("big_data")
    conf = conf.setMaster("local[*]")
    sc= SparkContext(conf=conf)
    SQLContext   = SQLContext(sc)
    spark=SparkSession(sc)
    args = sys.argv
    inp = args[1]
    out = args[2]
    main(sc,SQLContext,inp,out)

