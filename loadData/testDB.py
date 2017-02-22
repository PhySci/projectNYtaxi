#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 12:37:33 2017

@author: frodos
"""

from sqlalchemy import create_engine, Table, Column, MetaData
import pandas as pd

DB_name = 'ML_project';
user = 'frodos';
passwd = 'Secret';


def main():
    engine = create_engine('postgresql://user:user@localhost/ML_project2')

    
    if not engine.dialect.has_table(engine, 'trips'):  # If table don't exist, Create.
        metadata = MetaData(engine)
        # Create a table with the appropriate Columns
        print('No table')
        user = Table('user', metadata,
            Column('user_id', Integer, primary_key=True),
            Column('user_name', String(16), nullable=False),
            Column('email_address', String(60)),
            Column('password', String(20), nullable=False))
        
        trip = Table('trip',metadata,
                     Column('id',Integer, primary_key=True),
                     Column('VendorID',Integer),
                     Column('tpep_pickup_datetime',DateTime),
                     Column('tpep_dropoff_datetime',DateTime),
                     Column('passenger_count',Integer),
                     Column('trip_distance',Float),
                     Column('pickup_longitude',Float),
                     Column('pickup_latitude',Float),
                     Column('RatecodeID',Integer),
                     Column('store_and_fwd_flag',Integer),
                     Column('dropoff_longitude',Float),
                     Column('dropoff_latitude',Float),
                     Column('payment_type',Integer),
                     Column('fare_amount',Float),
                     Column('extra,mta_tax',Float),
                     Column('tip_amount',Float),
                     Column('tolls_amount',Float),
                     Column('improvement_surcharge',Float),
                     Column('total_amount',Float),
                     Column('duration',DateTime),
                     Column('regions',Integer)
                     );
        metadata.create_all(engine)
        
if __name__ == '__main__':
    main();    
        
        
        
        
        
        
        
        
        
        
        
        