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
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

# define constants
dataPath = '/home/frodos/anaconda/work/projectNYtaxi/rawData';
tableName = 'trip';

Base = declarative_base();
                       
paymentDict = {'Credit':1,'CREDIT':1,'CRD':1,
               'CASH':2,'Cash':2,'CSH':2,
               'NOC':3,
               'DIS': 4,
               'UNK': 5};

vendorDict = {'CMT':1,'VTS': 2}

class trip2(Base):
    __tablename__ =tableName
    id = Column(Integer, primary_key=True)
    VendorID = Column(String)
    trip_pickup_datetime = Column(DateTime)
    trip_dropoff_datetime = Column(DateTime)
    passenger_count = Column(Integer)
    trip_distance = Column(Float)
    pickup_longitude = Column(Float)
    pickup_latitude = Column(Float)
    RatecodeID = Column(Integer)
    store_and_fwd_flag = Column(String)
    dropoff_longitude = Column(Float)
    dropoff_latitude = Column(Float)
    payment_type = Column(Integer)
    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    improvement_surcharge = Column(Float)
    total_amount = Column(Float)
    regions = Column(Integer)
    

def checkDB(engine):
    if  not engine.dialect.has_table(engine, tableName):  # If table don't exist, Create.
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
    # get list of files
    fList = os.listdir(dataPath);
    
    paymentTypes  = set();
    vendors = set();
    ratecodes = set();                   

    colName = ['VendorID','trip_pickup_datetime','trip_dropoff_datetime',
                          'passenger_count','trip_distance','pickup_longitude','pickup_latitude',
                          'RatecodeID','store_and_fwd_flag','dropoff_longitude','dropoff_latitude',
                          'payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount',
                          'total_amount'];

    #loop over file list
    for fName in fList:
    #if True:
    #    fName = fList[0];
        print fName
        fullName = dataPath+'/'+fName;
        #df = pd.read_csv(fullName,nrows = 10,names = colName,header = 1);
        df = pd.read_csv(fullName,usecols = ['vendor_id',' payment_type',' rate_code'],nrows = 100);
        
        df['vendor_id'] = df['vendor_id'].map(vendorDict);
        df[' payment_type'] = df[' payment_type'].map(paymentDict);
             
        for x in df[' payment_type'].unique():
               paymentTypes.add(x);
                               
        for x in df['vendor_id'].unique():
               vendors.add(x);
                          
        for x in df[' rate_code'].unique():
               ratecodes.add(x);                  
                     
                     
           
        #print df['payment_type']             
        #df['payment_type'] = df['payment_type'].map(paymentDict)   
    
        #print df['payment_type']   
                  
        
        #print(df.shape)        
print 'Payments types'
print paymentTypes
print '     '

print 'Vendors'
print vendors
print'         '

print 'Rate codes'
print ratecodes