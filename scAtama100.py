# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 19:48:44 2019

@author: kocak
"""
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
import sys
import os

#path = os.path.dirname(os.path.abspath(__file__))+"\\"
path = "C:/Users/DELL/Desktop/Bilgin/"

hatlar = [100]
arac_nos = []
for day in range(1):
    day = 4
    sc30 = pd.read_csv(path+"SmartCardData/sc"+str(day+1)+"oct.csv")
    for i in hatlar:
    
        lid = str(i)[0:-1]
        slid = str(i)[-1]     
        sc30_selected = sc30[(sc30["HAT_NO"]==int(lid))&(sc30["ALT_HAT_NO"]==int(slid))&(sc30["ENLEM"]>0)&(sc30["BOYLAM"]>0)]
        sc30_selected = gpd.GeoDataFrame(sc30_selected,crs={'init':'epsg:4326'},geometry=[Point(x, y) for x,y in zip(sc30_selected.BOYLAM, sc30_selected.ENLEM)])
        sc30_selected = sc30_selected.to_crs(epsg=3857)
        for vid, group in sc30_selected.groupby(by=["ARAC_NO"]):
            for i,row in group.iterrows():
                if group.iloc[0].geometry.distance(row.geometry) >= 400:
                    arac_nos.append(row["ARAC_NO"])
                    print("arac_no: "+str(row["ARAC_NO"])+", " + str(group.iloc[0].geometry.distance(row.geometry))+" m")
                    break
                
#%%