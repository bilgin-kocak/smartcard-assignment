# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 15:44:26 2020

@author: DELL
"""

# This scripts divedes raw sc data into days

import pandas as pd
import os

def divide2days(year, months):
    directory = os.getcwd()+"/"
    path = directory + '/ulasım_veri/'
    files = [file[2] for file in os.walk(path)][0]
    #year ="2019"
    #months = ["03"]
    
    files = [files[0]]
    
    i = 0
    for file in files:
        
        raw_data = pd.read_csv(path+file,sep=';')
        raw_data["ENLEM"] = pd.to_numeric(raw_data['ENLEM'].astype(str).str.replace(',','.'))
        raw_data["BOYLAM"] = pd.to_numeric(raw_data['BOYLAM'].astype(str).str.replace(',','.'))
        m = months[i]
        for day in range(1,31):
            try:
                filetered_data = raw_data[(pd.to_datetime(raw_data["TIMESTAMP"]) >= pd.to_datetime(year+'-'+m+'-'+str(day)+' 02:59:59')) & (pd.to_datetime(raw_data["TIMESTAMP"]) < pd.to_datetime(year+'-'+m+'-'+str(day+1)+' 03:00:00')) & (raw_data["ENLEM"] > 0) & (raw_data["BOYLAM"] > 0)]
                if not os.path.exists(directory+"RawSCData/"+year+"/"+m+"/"):
                    os.makedirs(directory+"RawSCData/"+year+"/"+m+"/")
                filetered_data.to_csv(directory+"RawSCData/"+year+"/"+m+"/sc"+str(day)+".csv")
            except:
                continue
        i +=1
