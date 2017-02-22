#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 19 16:09:45 2017

@author: frodos
"""

# import of modules
import requests
import shutil
import itertools


def mainF():

    urlBase = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata';

    # create list of files
    #years = range(2009,2016);
    years = range(2009,2016);
    months = ['02','03','04','05','06','07','08','09','10','11','12'];

    sets = itertools.product(years,months); 
                        
    for (yr,mh) in sets:
        if ((yr==2009) & (mh=='01')):
            continue
        if ((yr==2009) & (mh=='02')):
            continue
        url = urlBase+'_'+str(yr)+'-'+str(mh)+'.csv'; 
        print url
        print download_file(url);

def download_file(url):
    local_filename = url.split('/')[-1]
    r = requests.get(url, stream=True);
    try:
        with open(local_filename, 'wb') as f:
            print r.status_code
            shutil.copyfileobj(r.raw, f);
        r.close();
        return local_filename
       
    except Exception as inst:
        r.close();
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst)                   
    
if __name__ == "__main__":
    mainF();


