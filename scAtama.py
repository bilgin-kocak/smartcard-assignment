# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 19:48:44 2019

@author: kocak
"""
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from tramAtama import tramAtama
#path = os.path.dirname(os.path.abspath(__file__))+"\\"
import os
path = os.getcwd() + "/"
#sys.path.append(path)
import scutils
#%%

def scAtama(year, month):
    #%%
    files = [file[2] for file in os.walk(path+"line_split_metu_corrected_v2/")][0]
    files = [file for file in files if file[-4:]==".shp"]
    hats = [int(file.split("_")[1]) for file in files]
    
    
    #month = "03"
    #year = "2019"
    
    #hatlar = [450]
    file = open("errors.txt","a") 
    
    for i in hats:
        for day in range(30):
            try:
                sc30 = pd.read_csv(path+"RawSCData/" + year + "/" + month+"/sc"+str(day+1)+".csv", encoding="ISO-8859-1")
            except FileNotFoundError:
                print("sc"+str(day+1)+".csv file does not exist!")
                continue
            lid = str(i)[0:-1]
            slid = str(i)[-1]
            #pathShape = path+"Hatlar/ULID_"+lid+slid+"_SP_M.shp"
            
            pathShape = path+"line_split_metu_corrected_v2/ULID_"+lid+slid+"_SP_M.shp"
            
            road = gpd.read_file(pathShape)
            try:
                road.rename(columns={'D_NO_12':'D_NO_2','Shape_leng':'Sape_lengh'}, inplace=True)
                road["USID"] = "ULID"+lid+slid+"_"+road["D_SIRA_1"].map(str)+"_"+road["D_SIRA_2"].map(str)+"_"+road["D_NO_1"].map(str)+"_"+road["D_NO_2"].map(str)
                road["X1"] = pd.to_numeric(road['X1'].astype(str).str.replace(',','.'))
                road["X2"] = pd.to_numeric(road["X2"].astype(str).str.replace(',','.'))
                road["Y1"] = pd.to_numeric(road["Y1"].astype(str).str.replace(',','.'))
                road["Y2"] = pd.to_numeric(road["Y2"].astype(str).str.replace(',','.'))
                
                sc30_selected = sc30[(sc30["HAT_NO"]==int(lid))&(sc30["ALT_HAT_NO"]==int(slid))]
                sc30_selected["ShiftID"] = sc30_selected["HAT_NO"].map(str)+sc30_selected["ALT_HAT_NO"].map(str)+"_"+sc30_selected["ARAC_NO"].map(str)
                if sc30_selected.shape[0] == 0:
                    continue
            
                scutils.resetIndex(sc30_selected)
                
                sc30_sel_segment = scutils.createSegmentBase(sc30_selected,road)
                
                sc30_sel_segment1 = scutils.createMoveGroup(sc30_sel_segment)
                
                scutils.getLocalDurak(sc30_sel_segment1)
                
                sc30_sel_segment1 = scutils.getDirections(sc30_sel_segment1)
                sc_segments_w_tripID = scutils.getTripID(sc30_sel_segment1)
                
                sc_selected_result = scutils.assignScData(sc_segments_w_tripID,sc30_selected)
                
                
                if not os.path.exists(path+"SmartCardResults_"+year+"/"+month+"/ULID"+lid+slid):
                    os.makedirs(path+"SmartCardResults_"+year+"/"+month+"/ULID"+lid+slid)
                sc_selected_result.to_csv(path+"SmartCardResults_"+year+"/"+month+"/ULID"+lid+slid+"/ULID"+lid+slid+"_"+str(day+1)+"oct.csv")
                sc_selected_result.to_excel(path+"SmartCardResults_"+year+"/"+month+"/ULID"+lid+slid+"/ULID"+lid+slid+"_"+str(day+1)+"oct.xlsx")
            except:
                file.write(lid+slid+"_"+str(day+1)+'\n')  
    file.close()
    #%%
    # Post Process Part
    #month = "10"
    #year = "2018"
    
    
    directory = path + "SmartCardResults_"+year+"/"+month
    folders = [x[0] for x in os.walk(directory)]
    atanmis_hatlar = []
    for i in range(len(folders)):
        if i == 0:
            tempStr = folders[i]
            continue
        folder_name = folders[i].replace(tempStr,"")
        folder_name = folder_name.replace("\\ULID","")
        atanmis_hatlar.append(int(folder_name))
        
    file = open("logForPostProcessing_tumHatlar1.txt","a") 
    hatalıHatlar = [11,50,51,91,110,112,130,170,190,270,500,550,680,410,990,1010,1240,1070,710,\
                    730,850,880,890,910]
    def notExist(arac_no,tripID,history):
        res = [history[j][0] != arac_no or history[j][1] != tripID for j in range(len(history))]
        # If there exit one False value return False False means farkli degil.
        return all(res)
    def getDULTripID(arac_no,tripID,history):
        for j in range(len(history)):
            if history[j][0] == arac_no and history[j][1] == tripID:
                return history[j][2]
        return -1
    
    for i in atanmis_hatlar:
        for day in range(30):
    
            lid = str(i)[0:-1]
            slid = str(i)[-1]
    
            pathShape = path+"line_split_metu_corrected_v2/ULID_"+lid+slid+"_SP_M.shp"
            
            road = gpd.read_file(pathShape)
            road.rename(columns={'D_NO_12':'D_NO_2','Shape_leng':'Sape_lengh'}, inplace=True)
            
            stopIDs = {}
            for ind, r in road.iterrows():
                stopIDs[int(r["D_SIRA_2"])] = r["D_NO_2"]
            for ind, r in road.iterrows():
                stopIDs[int(r["D_SIRA_1"])] = r["D_NO_1"]
            stopIDs[(pd.to_numeric(road["D_SIRA_2"])).max() + 1] = stopIDs[(pd.to_numeric(road["D_SIRA_2"])).max()]
            stopIDs[(pd.to_numeric(road["D_SIRA_1"])).min() - 1] = stopIDs[(pd.to_numeric(road["D_SIRA_1"])).min()]
            try:
                pathForSc = path+"SmartCardResults_"+year+"/"+month+"/ULID"+lid+slid+"/ULID"+lid+slid+"_"+str(day+1)+"oct.csv"
                scResult = pd.read_csv(pathForSc, encoding="ISO-8859-1")
                scResult.drop("ShiftID",axis = 1, inplace = True)
                scResult.sort_values(by=["TIMESTAMP"], inplace=True)
                #scResult["Date"]
                history = []
                dULTripID = 0
                for index, row in scResult.iterrows():
                    if index == 0:
                        dULTripID += 1
                        history.append([row["ARAC_NO"], row["tripID"], dULTripID])
                    scResult.loc[index,"Date"] = row["TIMESTAMP"].split()[0]
                    scResult.loc[index,"Time"] = row["TIMESTAMP"].split()[1]
                    scResult.loc[index,"ULID_corr"] = "ULID"+str(i)
                    scResult.loc[index,"ShiftID"] = str(i) + "_"+ str(row["ARAC_NO"])
                    scResult.loc[index,"STripID"] = row["tripID"]
                    if notExist(row["ARAC_NO"], row["tripID"], history) and row["Stat"] != 0:
                        dULTripID += 1
                        history.append([row["ARAC_NO"], row["tripID"], dULTripID])
                        scResult.loc[index,"DULTripID"] = dULTripID
                    else:
                        scResult.loc[index,"DULTripID"] = getDULTripID(row["ARAC_NO"], row["tripID"], history)
                    scResult.loc[index,"AsgnStopOrd"] = row["AssignedStopID"]
                    if row["Stat"] != 0:
                        scResult.loc[index,"AsgnStopID"] = stopIDs[int(row["AssignedStopID"])]
                    scResult.loc[index,"AsgnStat"] = row["Stat"]
                
                scResult.drop(["Unnamed: 0","Unnamed: 0.1" , "AssignedStopID", "Stat", "tripID"],axis=1, inplace = True)
                
                if not os.path.exists(path+"SmartCardResults_"+year+"_post/"+month+"/ULID"+lid+slid):
                    os.makedirs(path+"SmartCardResults_"+year+"_post/"+month+"/ULID"+lid+slid)
                scResult.to_csv(path+"SmartCardResults_"+year+"_post/"+month+"/ULID"+lid+slid+"/ULID"+lid+slid+"_"+str(day+1)+".csv")
                scResult.to_excel(path+"SmartCardResults_"+year+"_post/"+month+"/ULID"+lid+slid+"/ULID"+lid+slid+"_"+str(day+1)+".xlsx")
            except:
                file.write(lid+slid+"_"+str(day+1)+'\n') 
    file.close()
    
    #%%
    
    tramAtama(year, month)