#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 22:40:15 2017

Read csv files, clean it, upload to DB

@author: frodos
"""

import pandas as pd
import numpy  as np
import os
from scipy.stats import binned_statistic_2d
from sqlalchemy import create_engine, Table, Column, MetaData, PrimaryKeyConstraint
from sqlalchemy import Integer, DateTime, Float, String

# define constants
dataPath = '/home/frodos/anaconda/work/projectNYtaxi/rawData';
tableName = 'trip';


def checkDB(engine):
    if  True: #not engine.dialect.has_table(engine, tableName):  # If table don't exist, Create.
        metadata = MetaData(engine)
        # Create a table with the appropriate Columns        
        trip = Table(tableName,metadata,
                     Column('id',Integer, primary_key=True),
                     Column('VendorID',String),
                     Column('trip_pickup_datetime',DateTime),
                     Column('trip_dropoff_datetime',DateTime),
                     Column('passenger_count',Integer),
                     Column('trip_distance',Float),
                     Column('pickup_longitude',Float),
                     Column('pickup_latitude',Float),
                     Column('RatecodeID',Integer),
                     Column('store_and_fwd_flag',String),
                     Column('dropoff_longitude',Float),
                     Column('dropoff_latitude',Float),
                     Column('payment_type',Integer),
                     Column('fare_amount',Float),
                     Column('extra',Float),
                     Column('mta_tax',Float),
                     Column('tip_amount',Float),
                     Column('tolls_amount',Float),
                     Column('improvement_surcharge',Float),
                     Column('total_amount',Float),
                     Column('regions',Integer));
        metadata.create_all(engine);
                           
def cleanData(df,regs):

    # drop trips without passengers
    inds = df[df.passenger_count ==0].index;
    print 'No passengers: ',len(inds),' trips';
    df.drop(inds,inplace = True);
    
    
    inds = df[df.trip_dropoff_datetime == df.trip_pickup_datetime].index;
    print 'Zero duration: ',len(inds),' trips';
    df.drop(inds,inplace = True);
           
    #round 
    #df['tpep_pickup_datetime'] = pd.DatetimeIndex(df['tpep_pickup_datetime']).round('1H') ;
      
           
    # delete empty trips
    inds = df[df.trip_distance == 0].index;
    print 'Zero distance: ',len(inds),' trips';
    df.drop(inds,inplace = True);
    
    # find corners of the New-York rectangular
    east = regs.loc[:,'east'].max();
    west = regs.loc[:,'west'].min();
    south = regs.loc[:,'south'].min();
    north = regs.loc[:,'north'].max();
    
    # find outliers
    inds = df[(df.pickup_longitude>east) | (df.pickup_longitude<west) | (df.pickup_latitude>north) | (df.pickup_latitude<south)].index;
    df.drop(inds,inplace=True);
    return df                             
    
                    

if __name__ =='__main__':
    # connect to DB
    engine = create_engine('postgresql://user:user@localhost/ML_project2');
    checkDB(engine);
           
    #load coordinates of regions
    regs = pd.read_csv("../data/regions.csv",sep=";",index_col='region');

                    
    X = np.unique([regs.west,regs.east]);
    Y = np.unique([regs.south,regs.north]);
    
    # get list of files
    fList = os.listdir(dataPath);
    
    #loop over file list
    for fName in fList:  
        fullName = dataPath+'/'+fName;
        print(fullName);
        df = pd.read_csv(fullName,parse_dates = [1,2]);
        df.columns = ['VendorID','trip_pickup_datetime','trip_dropoff_datetime',
                            'passenger_count','trip_distance','pickup_longitude','pickup_latitude',
                            'RatecodeID','store_and_fwd_flag','dropoff_longitude','dropoff_latitude',
                            'payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount',
                            'improvement_surcharge','total_amount'];
        df = cleanData(df,regs);
    
           
        # Сопоставляем регион и координаты посадки для каждой поездки.
        # Добавляем данные о номере региона как новый столбец.
        [statistic, xEdge, yEdge, binNum] = binned_statistic_2d(df.pickup_longitude,df.pickup_latitude,df.index,statistic = 'count',bins = [X,Y],expand_binnumbers = True);
        df = df.assign(regions = (binNum[0,:]-1)*(X.size-1)+binNum[1,:]);
                  
        print(df.shape)
        df.to_sql(tableName,engine,if_exists='append',index=False,chunksize=1000000);
