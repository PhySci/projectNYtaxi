#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 19 20:33:02 2017

@author: frodos
"""


import pandas as pd
from scipy.stats import binned_statistic_2d
import numpy as np


def main():
    fName = 'yellow_tripdata_2009-01.csv';
    df = pd.read_csv(fName);
    
    df['tpep_pickup_datetime'] = pd.DatetimeIndex(df['tpep_pickup_datetime']).round('1H') ;
      
    
    print 'Here';



if __name__ == 'main':
    main();
        
def cleadData(df):
     
    #round 
    df['tpep_pickup_datetime'] = pd.DatetimeIndex(df['tpep_pickup_datetime']).round('1H') ;
      
    # drop trips without passengers
    inds = df[df.passenger_count ==0].index;
    print 'No passengers: ',len(inds),' trips';
    df.drop(inds,inplace = True);
    
    # create additional column - duration of the trip
    df = df.assign(duration = df.tpep_dropoff_datetime - df.tpep_pickup_datetime);
    # delete empty trips
    inds = df[df.duration == pd.to_timedelta('0 days')].index;
    print 'Zero duration: ',len(inds),' trips';
    df.drop(inds,inplace = True);
           
    # delete empty trips
    inds = df[df.trip_distance == 0].index;
    print 'Zero distance: ',len(inds),' trips';
    df.drop(inds,inplace = True);
    
           
    #load coordinates of regions
    regs = pd.read_csv("data\\regions.csv",sep=";",index_col='region');
    # find corners of the New-York rectangular
    east = regs.loc[:,'east'].max();
    west = regs.loc[:,'west'].min();
    south = regs.loc[:,'south'].min();
    north = regs.loc[:,'north'].max();
    
    # find outliers
    inds = df[(df.pickup_longitude>east) | (df.pickup_longitude<west) | (df.pickup_latitude>north) | (df.pickup_latitude<south)].index;
    df.drop(inds,inplace=True);
           
    # Сопоставляем регион и координаты посадки для каждой поездки.
    # Добавляем данные о номере региона как новый столбец.
    X = np.unique([regs.west,regs.east]);
    Y = np.unique([regs.south,regs.north]);

    [statistic, xEdge, yEdge, binNum] = binned_statistic_2d(df.pickup_longitude,df.pickup_latitude,df.index,statistic = 'count',bins = [X,Y],expand_binnumbers = True);
    df = df.assign(regions = (binNum[0,:]-1)*(X.size-1)+binNum[1,:]);
    return df  
      
    