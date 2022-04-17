# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 10:36:03 2019

@author: kocak
"""
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np

def createSegmentBase(sc,test):

    

    
    #Read selected file path
    path = "C:/Users/kocak/OneDrive/Masaüstü/reag/ULID443_Guzargah_Durak/ULID443_Durak_split_v2.shp"
    #test = gpd.read_file(pathShapeFile)
    
    
  
    # Create a column that is from Point(lon,lat) format
    sc = gpd.GeoDataFrame(sc,geometry=[Point(x, y) for x,y in zip(sc.BOYLAM, sc.ENLEM)])
    
    # Add 100m,50m,30m,10m line buffer to the line 
    buffer_ = test.copy()
    buffer_["geometry_100"] = ""
    buffer_["geometry_50"] = ""
    buffer_["geometry_30"] = ""
    buffer_["geometry_10"] = ""
    buffer_.geometry_100 = buffer_['geometry'].buffer(0.0009009009)
    buffer_.geometry_50 = buffer_['geometry'].buffer(0.00045045045)
    buffer_.geometry_30 = buffer_['geometry'].buffer(0.00022522522)
    buffer_.geometry_10 = buffer_['geometry'].buffer(0.00009009009)
    buffer_temp = gpd.GeoDataFrame(buffer_,geometry = buffer_["geometry_100"])
    
    # Flag to see whether the segment is in 100m buffer of a segment
    flag = 0
    
    
    # list to be list of dictionaries (dictionaries will be converted to corresponding dataframe row)
    lst = []
    
    for j,group in sc.groupby("ShiftID"):
        for k, c in group.iterrows():
            # For each buffer geometry in the segment
            for i,b in buffer_.iterrows():
                # Initialize a dictionary
                d = {}
                d["INDEX"] = c["INDEX"] 
                # If 100m buffer contains point's geometry  - (contains is a geopandas function)
                if (b.geometry_100).contains(c["geometry"]):
                    flag = 1
                    d["in_buffer_100"] = True
                    d["VID"] = c["ARAC_NO"]
                    d["TIMESTAMP"] = c["TIMESTAMP"]
                    d["POINT_LAT"] = c["ENLEM"]
                    d["POINT_LON"] = c["BOYLAM"]
                    d["ShiftID"] = c["ShiftID"]
                    d["BUFFER1_LAT"] = b["Y1"]
                    d["BUFFER1_LON"] = b["X1"]
                    d["BUFFER2_LAT"] = b["Y2"]
                    d["BUFFER2_LON"] = b["X2"]
                    d["DURAK_SIRA_1"] = b["D_SIRA_1"]
                    d["DURAK_SIRA_2"] = b["D_SIRA_2"]
                    d["DURAK_NO_1"] = b["D_NO_1"]
                    d["DURAK_NO_2"] = b["D_NO_2"]
                    d["USID"] = b["USID"]
                    d["SegLength"] = b["Sape_lengh"]
                    
    
                else:
                    d["in_buffer_100"] = False
                    d["VID"] = c["ARAC_NO"]
                    d["TIMESTAMP"] = c["TIMESTAMP"]
                    d["POINT_LAT"] = c["ENLEM"]
                    d["POINT_LON"] = c["BOYLAM"]
                    d["ShiftID"] = c["ShiftID"]
                    d["BUFFER1_LAT"] = b["Y1"]
                    d["BUFFER1_LON"] = b["X1"]
                    d["BUFFER2_LAT"] = b["Y2"]
                    d["BUFFER2_LON"] = b["X2"]
                    d["DURAK_SIRA_1"] = b["D_SIRA_1"]
                    d["DURAK_SIRA_2"] = b["D_SIRA_2"]
                    d["DURAK_NO_1"] = b["D_NO_1"]
                    d["DURAK_NO_2"] = b["D_NO_2"]
                    d["USID"] = b["USID"]
                    d["SegLength"] = b["Sape_lengh"]
                    
                    
                # If 50m buffer contains point's geometry  - (contains is a geopandas function)
                if (b.geometry_50).contains(c["geometry"]):
                    d["in_buffer_50"] = True
                else:
                    d["in_buffer_50"] = False
                    
                    
                # If 30m buffer contains point's geometry  - (contains is a geopandas function)
                if (b.geometry_30).contains(c["geometry"]):
                    d["in_buffer_30"] = True
                else:
                    d["in_buffer_30"] = False      
                    
                    
                # If 10m buffer contains point's geometry
                if (b.geometry_10).contains(c["geometry"]):
                    d["in_buffer_10"] = True
                else:
                    d["in_buffer_10"] = False
                # If the point is in 100m buffer, append the point to the list
                if flag == 1:
                    #print(d)
                    flag = 0
                    lst.append(d)
                    #print(lst[-1])
            
    # List of dictionaries to pandas dataframe
    df = pd.DataFrame(lst)
    return df
def getIndex(df):
    for i, group in df.groupby(["USID","VID"]):
        group = group.sort_values(["TIMESTAMP"])
        index = 1
        for j, row in group.iterrows():
            df.loc[j,"INDEX"] = index
            index = index + 1
    
    return df

def createMoveGroup(df):

    # Group points in 100m buffer by SegmentID, VehicleID and DriverID
    df_grouped = df.groupby(["USID","VID"])
    
    #List of dictionaries
    rows = []
    
    # To see it is the first row of the whole dataframe
    first_row = True
    
    # Will be append to the rows list
    new_row = {}
    
    # To see whether there is a row to be inserted
    flag = 0
    
    # Initialize first movegroup number
    last_movement = 1
    movement = 1
    for i,g in df_grouped:
        # Sort current group by index
        g_sorted = g.sort_values(by = ["INDEX"])
        # Initialize prev_index to add previous index
        prev_index = -99
        
        # To see if it is the first member of the group
        new_group = True
        movement = 1
        
        # Travel through the sorted group
        for j, row in g_sorted.iterrows():
            
            # If it is the first row of the group but not of the dataframe (to insert the last element of the previous group)
            if new_group == True and first_row == False and flag == 1:
                #Add values of last row of the group
                new_row["L_IND"] = last_row["INDEX"]
                new_row["LP2FS"] = calculateDistance([last_row["POINT_LAT"],last_row["POINT_LON"]],[last_row["BUFFER1_LAT"],last_row["BUFFER1_LON"]])
                new_row["LP2LS"] = calculateDistance([last_row["POINT_LAT"],last_row["POINT_LON"]],[last_row["BUFFER2_LAT"],last_row["BUFFER2_LON"]])
                new_row["L_TS"] = last_row["TIMESTAMP"]
                new_row["LP_LAT"] = last_row["POINT_LAT"]
                new_row["LP_LON"] = last_row["POINT_LON"]
                new_row["SegLength"] = last_row["SegLength"]
                # Add ShiftID
                segid = "443_" + str(new_row["VID"])
                new_row["ShiftID"] = segid
                # Add movegroup information
                new_row["MVGRP"] = last_movement
                # Initialize values again for the new group
                last_movement = 1
                rows.append(new_row)
                new_row = {}
                usid_list = row["USID"].split("_")
                # Rearrange segmentID
                usid = usid_list[0] + "_" + usid_list[3] + "_" + usid_list[4] + "_" + usid_list[1] + "_" + usid_list[2]
                new_row["SEGTID"] = usid
                new_row["F_TS"] = row["TIMESTAMP"]
                new_row["VID"] = row["VID"]
                new_row["DURAK1_LAT"] = row["BUFFER1_LAT"]
                new_row["DURAK1_LON"] = row["BUFFER1_LON"]
                new_row["DURAK2_LAT"] = row["BUFFER2_LAT"]
                new_row["DURAK2_LON"] = row["BUFFER2_LON"]
                new_row["FP_LAT"] = row["POINT_LAT"]
                new_row["FP_LON"] = row["POINT_LON"]
                new_row["FP2FS"] = calculateDistance([row["POINT_LAT"],row["POINT_LON"]],[row["BUFFER1_LAT"],row["BUFFER1_LON"]])
                new_row["FP2LS"] = calculateDistance([row["POINT_LAT"],row["POINT_LON"]],[row["BUFFER2_LAT"],row["BUFFER2_LON"]])
                new_row["F_IND"] = row["INDEX"]
                
                # There are some values to be inserted
                flag = 1
                # We are not at the first value of the group anymore
                new_group = False
                
            # If there is a jump between previous index and current index, previous index is the last index of the movegroup    
            elif row["INDEX"] - prev_index > 2:
                # Since prev_index = -99 for the first row,1 - (-99) = 100 > 2 (however, there is nothing to be inserted), then we should check whether there is a need to divide movegroups
                # If it is not the first row and there is a row to be inserted 
                if first_row == False and flag == 1:
    
                    # Append dictionary of the previous movegroup
                    new_row["L_IND"] = last_row["INDEX"]
                    new_row["LP2FS"] = calculateDistance([last_row["POINT_LAT"],last_row["POINT_LON"]],[last_row["BUFFER1_LAT"],last_row["BUFFER1_LON"]])
                    new_row["LP2LS"] = calculateDistance([last_row["POINT_LAT"],last_row["POINT_LON"]],[last_row["BUFFER2_LAT"],last_row["BUFFER2_LON"]])
                    new_row["L_TS"] = last_row["TIMESTAMP"]
                    new_row["LP_LAT"] = last_row["POINT_LAT"]
                    new_row["LP_LON"] = last_row["POINT_LON"]
                    new_row["SegLength"] = last_row["SegLength"]
                    # Find shift of the First and Last point of the movegroup
                    segid = "443_" + str(new_row["VID"])
                    #segid = "443_" + str(new_row["VID"]) + "_" + str(new_row["DID"]) + "_" + str(new_row["FIRST_TS"] + "_" +str(new_row["LAST_TS"])) 
                    new_row["ShiftID"] = segid
                    new_row["MVGRP"] = movement
                    rows.append(new_row)
                    # Initialize new movegroup of the group
                    movement = movement + 1
                    last_movement = movement
                    flag = 0
                    new_row = {}
                    
                # Edit SegmentID of the current group
                usid_list = row["USID"].split("_")
                usid = usid_list[0] + "_" + usid_list[3] + "_" + usid_list[4] + "_" + usid_list[1] + "_" + usid_list[2]
                new_row["SEGTID"] = usid
                
                new_row["F_TS"] = row["TIMESTAMP"]
                new_row["VID"] = row["VID"]
                new_row["FP_LAT"] = row["POINT_LAT"]
                new_row["FP_LON"] = row["POINT_LON"]
                new_row["DURAK1_LAT"] = row["BUFFER1_LAT"]
                new_row["DURAK1_LON"] = row["BUFFER1_LON"]
                new_row["DURAK2_LAT"] = row["BUFFER2_LAT"]
                new_row["DURAK2_LON"] = row["BUFFER2_LON"]
                new_row["FP2FS"] = calculateDistance([row["POINT_LAT"],row["POINT_LON"]],[row["BUFFER1_LAT"],row["BUFFER1_LON"]])
                new_row["FP2LS"] = calculateDistance([row["POINT_LAT"],row["POINT_LON"]],[row["BUFFER2_LAT"],row["BUFFER2_LON"]])
                new_row["F_IND"] = row["INDEX"]
                # Now, there is something to append, then flag = 1
                flag = 1
                
            # Keep information about the previous row
            prev_index = row["INDEX"]
            last_row = row
            first_row = False
            new_group = False
    # If there is any value to be inserted, it should be inserted. 
    if flag == 1:
        new_row["L_IND"] = last_row["INDEX"]
        new_row["LP2FS"] = calculateDistance([last_row["POINT_LAT"],last_row["POINT_LON"]],[last_row["BUFFER1_LAT"],last_row["BUFFER1_LON"]])
        new_row["LP2LS"] = calculateDistance([last_row["POINT_LAT"],last_row["POINT_LON"]],[last_row["BUFFER2_LAT"],last_row["BUFFER2_LON"]])
        new_row["L_TS"] = last_row["TIMESTAMP"]
        new_row["LP_LAT"] = last_row["POINT_LAT"]
        new_row["LP_LON"] = last_row["POINT_LON"]
        new_row["SegLength"] = last_row["SegLength"]
        segid = "443_" + str(new_row["VID"]) 
        #segid = "443_" + str(new_row["VID"]) + "_" + str(new_row["DID"]) + "_" + str(new_row["FIRST_TS"]) + "_" +str(new_row["LAST_TS"])
        new_row["ShiftID"] = segid
        new_row["MVGRP"] = last_movement
        rows.append(new_row)
    rows_df = pd.DataFrame(rows).drop(columns = ["VID"])
    return rows_df
def resetIndex(df):
    df["INDEX"] = 0
    for i, group in df.groupby("ARAC_NO"):
        group = group.sort_values("TIMESTAMP")
        index = 1
        for j, row in group.iterrows():
            df.loc[j,"INDEX"] = index
            index = index +1
    return df
def calculateDistance(a,b):
    
    s_lat = float(a[0])
    s_lng = float(a[1])
    e_lat = float(b[0])
    e_lng = float(b[1])
    R = 6373.0
    
    s_lat = float(s_lat)*np.pi/180.0                      
    s_lng = np.deg2rad(float(s_lng))     
    e_lat = np.deg2rad(float(e_lat))                       
    e_lng = np.deg2rad(float(e_lng))  
    
    d = np.sin((e_lat - s_lat)/2)**2 + np.cos(s_lat)*np.cos(e_lat) * np.sin((e_lng - s_lng)/2)**2
    return 2 * R * np.arcsin(np.sqrt(d)) 
def getLocalDurak(df):
    for i, row in df.iterrows():
        df.loc[i,"ST1"] = int(row["SEGTID"].split("_")[3])
        df.loc[i,"ST2"] = int(row["SEGTID"].split("_")[4])
        df.loc[i,"#ofScData"] = int(row["L_IND"]) - int(row["F_IND"]) + 1
        df.loc[i,"FP_LP"] = str(row["F_IND"]) + "_" + str(row["L_IND"])
    return df
def calculateDifference(df):
    for i, row in df.iterrows():
        df.loc[i,"#ofScData"] = int(row["L_IND"]) - int(row["F_IND"]) + 1
def getDirections(df):
    df["Direction"] = -1
    for i,group in df.groupby("ShiftID"):
        group = group.sort_values(by=["ST1"])
        group["Direction"] = -1
        historyIndex = []
        direction = 0
        for index,row in group.iterrows():
            if group.loc[index,"Direction"] != -1:
                continue
            listIndexes = []
            for index1,row1 in group[group["Direction"]==-1].iterrows():
                listIndexes.append(index1)
            listForSearch = listIndexes[listIndexes.index(index):]
            F_IND = row["F_IND"]
            L_IND = row["L_IND"]
            for j in listForSearch:
                if F_IND <= group.loc[j,"F_IND"] and L_IND +1 >= group.loc[j,"F_IND"]:
                    historyIndex.append(j)
                    if F_IND < group.loc[j,"F_IND"]:
                        #group.loc[index,"Direction"] = 1
                        direction = 1
                    elif L_IND < group.loc[j,"L_IND"]:
                        #group.loc[index,"Direction"] = 1
                        direction = 1
                    F_IND = group.loc[j,"F_IND"]
                    L_IND = group.loc[j,"L_IND"]
                elif L_IND >= group.loc[j,"L_IND"] and F_IND -1 <= group.loc[j,"L_IND"]:
                    if direction != 1 : historyIndex = []
                    break
            if direction == 1:
                group.loc[historyIndex,"Direction"] = 1
                df.loc[historyIndex,"Direction"] = 1
                historyIndex = []
                direction = 0       
    return df
def getTripID(dff2):
    dff3 = dff2[dff2["Direction"]==1]
    for i, group in dff3.groupby("ShiftID"):
        listIndexes = []
        group = group.sort_values(["F_TS","ST1"])
        for index1,row1 in group.iterrows():
            listIndexes.append(index1)
        firstRow = True
        for index,row in group.iterrows():
            if firstRow:
                tripID = 1
                group.loc[index,"TripID"] = tripID
                dff3.loc[index,"TripID"] = tripID
                fs = row["ST1"]
                firstRow = False
                continue
            if row["ST1"] < fs:
                listForSearch = listIndexes[listIndexes.index(index):]
                ## sonrandan eklendi
#                if not listForSearch:
#                    continue
                ## sondradan eklenddi
                if group.loc[listForSearch[0],"F_IND"] == group.loc[listForSearch[1],"F_IND"] and \
                group.loc[listForSearch[0],"L_IND"] == group.loc[listForSearch[1],"L_IND"] and \
                group.loc[listForSearch[1],"ST1"] > fs:
                    tripID +=0
                    group.loc[index,"TripID"] = tripID + 1
                    group.loc[listForSearch[1],"TripID"] = tripID
                    dff3.loc[index,"TripID"] = tripID + 1
                    dff3.loc[listForSearch[1],"TripID"] = tripID
                    fs = row["ST1"]
                    continue
                else:
                    tripID +=1
            group.loc[index,"TripID"] = tripID
            dff3.loc[index,"TripID"] = tripID
            fs = row["ST1"]
    
    return dff3      
def assignScData(sc_segments_w_tripID,sc_selected):
    sc_segments_w_tripID["#ofPassengers"] = 0
    sc_segments_w_tripID["#ofPassenger@FS"] = 0 
    sc_segments_w_tripID["#ofPassenger@LS"] = 0 
    sc_segments_w_tripID["#ofPassenger@Route"] = 0 

    sc_segments_FP = gpd.GeoDataFrame(index=sc_segments_w_tripID.index,crs={'init':'epsg:4326'},geometry=[Point(x, y) for x,y in zip(sc_segments_w_tripID.DURAK1_LON, sc_segments_w_tripID.DURAK1_LAT)])
    sc_segments_LP = gpd.GeoDataFrame(index=sc_segments_w_tripID.index,geometry=[Point(x, y) for x,y in zip(sc_segments_w_tripID.DURAK2_LON, sc_segments_w_tripID.DURAK2_LAT)])
    sc_segments_FP["geometry50_FP"] = sc_segments_FP["geometry"].buffer(0.00045045045)
    sc_segments_FP["geometry100_FP"] = sc_segments_FP["geometry"].buffer(0.0009009009)
    sc_segments_LP["geometry50_LP"] = sc_segments_LP["geometry"].buffer(0.00045045045)
    sc_segments_LP["geometry100_LP"] = sc_segments_LP["geometry"].buffer(0.0009009009)
    sc_segments50_FP = gpd.GeoDataFrame(index=sc_segments_FP.index,crs={'init':'epsg:4326'},geometry = sc_segments_FP["geometry50_FP"] )
    sc_segments50_LP = gpd.GeoDataFrame(index=sc_segments_LP.index,crs={'init':'epsg:4326'},geometry = sc_segments_LP["geometry50_LP"] )
    
    sc_selected = gpd.GeoDataFrame(sc_selected,geometry =[Point(x, y) for x,y in zip(sc_selected.BOYLAM, sc_selected.ENLEM)] )
    sc_selected["AssignedStopID"] = "null"
    sc_selected["Stat"] = 0
    for shiftID, group in sc_segments_w_tripID.groupby("ShiftID"):
        group = group.sort_values(by=["F_TS"])
        scForGroup = sc_selected[(sc_selected["ARAC_NO"]== int(shiftID[4:7]))]
    #    gpsAracNo.append(int(shiftID[4:7]))
        isFirstRow = True
        for i, row in group.iterrows():
            sc_between_segment = scForGroup[(pd.to_datetime(scForGroup["TIMESTAMP"]) >= pd.to_datetime(row["F_TS"])) & (pd.to_datetime(scForGroup["TIMESTAMP"]) <= pd.to_datetime(row["L_TS"]))]
            sc_segments_w_tripID.loc[i,"#ofPassenger"] = sc_between_segment.shape[0]
            splitedSegtID = row["SEGTID"].split("_")
            if isFirstRow:
                indexesForFS = []
                indexesForLS = []
                indexesForRoute = []
                for index, row_sc in sc_between_segment.iterrows():
                    sc_selected.loc[index,"ShiftID"] = row["ShiftID"]
                    sc_selected.loc[index,"tripID"] = row["TripID"]
                    if sc_segments_FP.loc[i,"geometry50_FP"].contains(row_sc["geometry"]):
                        indexesForFS.append(index)
                        sc_selected.loc[index,"Stat"] = 1
                        sc_selected.loc[index,"AssignedStopID"] = splitedSegtID[3]
                        sc_between_segment.loc[index,"AssignedStopID"] = splitedSegtID[3]
                    elif sc_segments_LP.loc[i,"geometry50_LP"].contains(row_sc["geometry"]):
                        indexesForLS.append(index)
                        sc_selected.loc[index,"AssignedStopID"] = splitedSegtID[4]
                        sc_selected.loc[index,"Stat"] = 1
                        sc_between_segment.loc[index,"AssignedStopID"] = splitedSegtID[4]
                    else:
                        indexesForRoute.append(index)
                        distanceToFS = calculateDistance([row_sc["ENLEM"],row_sc["BOYLAM"]],[row["DURAK1_LAT"],row["DURAK1_LON"]])
                        distanceToLS = calculateDistance([row_sc["ENLEM"],row_sc["BOYLAM"]],[row["DURAK2_LAT"],row["DURAK2_LON"]])
                        distanceBetweenFS_LS = calculateDistance([row["DURAK1_LAT"],row["DURAK1_LON"]],[row["DURAK2_LAT"],row["DURAK2_LON"]])
                        sc_selected.loc[index,"Stat"] = 2
                        if distanceToFS <= 100 and distanceToLS > distanceBetweenFS_LS:
                            sc_selected.loc[index,"AssignedStopID"] = str(int(splitedSegtID[3])-1) #  "en-route_b_"+str(int(splitedSegtID[3])-1)+"_"+splitedSegtID[3]
                            sc_between_segment.loc[index,"AssignedStopID"] = "en-route_b_"+str(int(splitedSegtID[3])-1)+"_"+splitedSegtID[3]
                        elif distanceToLS <= 100 and distanceToFS > distanceBetweenFS_LS:
                            sc_selected.loc[index,"AssignedStopID"] = splitedSegtID[4] #"en-route_a_"+splitedSegtID[4]+"_"+str(int(splitedSegtID[4])+1)
                            sc_between_segment.loc[index,"AssignedStopID"] = "en-route_a_"+splitedSegtID[4]+"_"+str(int(splitedSegtID[4])+1)
                        else:
                            sc_selected.loc[index,"AssignedStopID"] =splitedSegtID[3] # "en-route_"+splitedSegtID[3]+"_"+splitedSegtID[4]
                            sc_between_segment.loc[index,"AssignedStopID"] = "en-route_"+splitedSegtID[3]+"_"+splitedSegtID[4]
                            
                isFirstRow = False
                sc_segments_w_tripID.loc[i,"#ofPassenger@FS"] = len(indexesForFS)
                sc_segments_w_tripID.loc[i,"#ofPassenger@LS"] = len(indexesForLS)
                sc_segments_w_tripID.loc[i,"#ofPassenger@Route"] = len(indexesForRoute)
                prevIndex = i
                continue
            indexesForFS = []
            indexesForLS = []
            indexesForRoute = []
            for index, row_sc in sc_between_segment.iterrows():
                sc_selected.loc[index,"ShiftID"] = row["ShiftID"]
                sc_selected.loc[index,"tripID"] = row["TripID"]
                if sc_segments_FP.loc[i,"geometry50_FP"].contains(row_sc["geometry"]):
                    indexesForFS.append(index)
                    sc_selected.loc[index,"AssignedStopID"] = splitedSegtID[3]
                    sc_selected.loc[index,"Stat"] = 1
                    sc_between_segment.loc[index,"AssignedStopID"] = splitedSegtID[3]
                elif sc_segments_LP.loc[i,"geometry50_LP"].contains(row_sc["geometry"]):
                    indexesForLS.append(index)
                    sc_selected.loc[index,"AssignedStopID"] = splitedSegtID[4]
                    sc_selected.loc[index,"Stat"] = 1
                    sc_between_segment.loc[index,"AssignedStopID"] = splitedSegtID[4]
                else:
                    indexesForRoute.append(index)
                    distanceToFS = calculateDistance([row_sc["ENLEM"],row_sc["BOYLAM"]],[row["DURAK1_LAT"],row["DURAK1_LON"]])
                    distanceToLS = calculateDistance([row_sc["ENLEM"],row_sc["BOYLAM"]],[row["DURAK2_LAT"],row["DURAK2_LON"]])
                    distanceBetweenFS_LS = calculateDistance([row["DURAK1_LAT"],row["DURAK1_LON"]],[row["DURAK2_LAT"],row["DURAK2_LON"]])
                    sc_selected.loc[index,"Stat"] = 2
                    if distanceToFS <= 100 and distanceToLS > distanceBetweenFS_LS:
                        sc_selected.loc[index,"AssignedStopID"] = str(int(splitedSegtID[3])-1) #"en-route_b_"+str(int(splitedSegtID[3])-1)+"_"+splitedSegtID[3]
                        sc_between_segment.loc[index,"AssignedStopID"] = "en-route_b_"+str(int(splitedSegtID[3])-1)+"_"+splitedSegtID[3]
                    elif distanceToLS <= 100 and distanceToFS > distanceBetweenFS_LS:
                        sc_selected.loc[index,"AssignedStopID"] = splitedSegtID[4] #"en-route_a_"+splitedSegtID[4]+"_"+str(int(splitedSegtID[4])+1)
                        sc_between_segment.loc[index,"AssignedStopID"] = "en-route_a_"+splitedSegtID[4]+"_"+str(int(splitedSegtID[4])+1)
                    else:
                        sc_selected.loc[index,"AssignedStopID"] = splitedSegtID[3]#"en-route_"+splitedSegtID[3]+"_"+splitedSegtID[4]
                        sc_between_segment.loc[index,"AssignedStopID"] = "en-route_"+splitedSegtID[3]+"_"+splitedSegtID[4]
            sc_segments_w_tripID.loc[i,"#ofPassenger@FS"] = len(indexesForFS)
            sc_segments_w_tripID.loc[i,"#ofPassenger@LS"] = len(indexesForLS)
            sc_segments_w_tripID.loc[i,"#ofPassenger@Route"] = len(indexesForRoute)
            prevIndex = i

    return sc_selected
# Tamamla
def postProcess(scResult,road):
    stopIDs = {}
    for ind, r in road.iterrows():
        stopIDs[int(r["D_SIRA_2"])] = r["D_NO_2"]
    for ind, r in road.iterrows():
        stopIDs[int(r["D_SIRA_1"])] = r["D_NO_1"]
   

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
        if notExist(row["ARAC_NO"], row["tripID"], history):
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