"""
# get data for the crowd regions
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from datetime import datetime

def main():

    regs = pd.read_csv("crowdRegs.csv",names = ['ind','regId'])
    engine = create_engine('postgresql://user:user@localhost/ML_project2')

    startDate = datetime(2016, 5, 1, 0, 0, 0)
    endDate = datetime(2016, 5, 31, 23, 59, 59)
    date_index = pd.date_range(startDate, endDate, freq='H')

    # create empty DataFrame
    df = pd.DataFrame(index = date_index)

    for row in regs.itertuples(name = None,index = False):
        region = row[1]
        print region

        # create query
        query = "SELECT date_trunc('hour',trip_pickup_datetime) AS date, count(trip_pickup_datetime) "
        query = query + "FROM trip WHERE regions= %(region)s AND "
        query = query + "trip_pickup_datetime BETWEEN %(startDate)s AND %(endDate)s "
        query = query + "GROUP BY date_trunc('hour',trip_pickup_datetime) "
        query = query + "ORDER BY date_trunc('hour',trip_pickup_datetime) "

        print 'Query'
        ts = pd.read_sql_query(query, engine, params={"region": region, "startDate": startDate, "endDate": endDate},index_col='date')
        print 'End query'

        # fill up zeros
        ts = ts.reindex(date_index, fill_value=0)
        df = df.merge(ts,left_index=True,right_index=True)

    df.to_pickle("crowdRegs4.pcl")

if __name__=="__main__":
    main()