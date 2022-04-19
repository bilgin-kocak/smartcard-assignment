# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 22:46:54 2020

@author: kocak
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 19:48:44 2019

@author: kocak
"""
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry import Point
path = os.getcwd() + "/"

def tramAtama(year, month):
    folder_name = "SmartCardResults_tram/"
#    year = "2019"
#    month = "05"
    hatlar = [100]
    road = pd.read_csv(path + "tram.csv")
#    road_info = pd.read_excel(path + "Oct_VID_ULID100.xlsx")
    road.loc[73:76,["KOORDINAT_X", "KOORDINAT_Y"]] = 32.0 
    # Important KOORDINAT_X is ENLEM and KOORDINAT_Y is BOYLAM wrong info is given to me
    roadForPlot = gpd.GeoDataFrame(road.loc[0:72],crs={'init':'epsg:4326'},geometry=[Point(x, y) for x,y in zip(road.loc[0:72].KOORDINAT_Y, road.loc[0:72].KOORDINAT_X)])
    #ax = roadForPlot.loc[0:56].plot(color="red")
    #for i,r in roadForPlot.loc[0:56].iterrows():
    #    plt.text(r["KOORDINAT_Y"],r["KOORDINAT_X"],r["DURAK_SIRASI"],fontsize=12,color="black")
    #plt.show()
    roadForPlot = roadForPlot.to_crs(epsg=3857)
    stop_info = roadForPlot.loc[27:56].to_crs(epsg=3857)
    stop_info = stop_info.append(roadForPlot.loc[72])
    ord2ID = dict(zip(stop_info.DURAK_SIRASI, stop_info.DURAK_NO))
    arac_no_durak_dict = {
            95:59,
            70:58,
            96:57,
            63:56,
            50:55,
            47:54,
            87:53,
            99:52,
            66:51,
            44:50,
            82:49,
            81:48,
            78:47,
            68:46,
            53:45,
            54:44,
            49:43,
            77:42,
            57:41,
            58:40,
            60:39,
            92:38,
            74:37,
            98:36,
            62:35,
            85:35,
            75:34,
            94:33,
            90:31,
            89:32,
            97:36,
            65:16
            }
    #hatlar = [450]
    for day in range(30):
        try:
            sc30 = pd.read_csv(path+"RawSCData/"+year+"/"+month+"/sc"+str(day+1)+".csv", encoding="ISO-8859-1")
        except FileNotFoundError:
            print("sc"+str(day+1)+".csv file does not exist!")
            continue
        for i in hatlar:
            lid = str(i)[0:-1]
            slid = str(i)[-1]
            
            sc30_selected = sc30[(sc30["HAT_NO"]==int(lid))&(sc30["ALT_HAT_NO"]==int(slid))]
            sc30_selected_gdf = gpd.GeoDataFrame(sc30_selected,crs={'init':'epsg:4326'},geometry=[Point(x, y) for x,y in zip(sc30_selected.BOYLAM, sc30_selected.ENLEM)] )
            sc30_selected_gdf = sc30_selected_gdf.to_crs(epsg=3857)
            sc30_selected_gdf["ARAC_NO"] = sc30_selected_gdf["ARAC_NO"].astype(int)
            sc30_selected_gdf = sc30_selected_gdf[sc30_selected_gdf["ARAC_NO"]<100]
            sc30_selected_gdf["Date"] = ""
            sc30_selected_gdf["Time"] = ""
            sc30_selected_gdf["ULID_corr"] = ""
            sc30_selected_gdf["ShiftID"] = ""
            sc30_selected_gdf["STripID"] = 0
            sc30_selected_gdf["DULTripID"] = 0
            for j, group in sc30_selected_gdf.groupby(by=["ARAC_NO"]):
    #            group.sort_values(by="TIMESTAMP",inplace=True)
    #            plt.scatter(group["BOYLAM"],group["ENLEM"],c="yellow")
    #            sum_distance = np.zeros((56-27+1))
    #            count = 0
                for index,r in group.iterrows():
    #                sum_distance += stop_info.geometry.distance(r.geometry)
                    sc30_selected_gdf.loc[index, "Date"] = sc30_selected_gdf.loc[index,"TIMESTAMP"].split(" ")[0]
                    sc30_selected_gdf.loc[index, "Time"] = sc30_selected_gdf.loc[index,"TIMESTAMP"].split(" ")[1]
                    
                    if j == 65:
                        sc30_selected_gdf.loc[index, "ULID_corr"] = "ULID102"
                        sc30_selected_gdf.loc[index, "ShiftID"] = "102_"+str(j)
                    else:
                        sc30_selected_gdf.loc[index, "ULID_corr"] = "ULID100"
                        sc30_selected_gdf.loc[index, "ShiftID"] = "100_"+str(j)
                    sc30_selected_gdf.loc[index,"AsgnStopOrd"] = arac_no_durak_dict[j]
                    sc30_selected_gdf.loc[index,"AsgnStopID"] = ord2ID[sc30_selected_gdf.loc[index,"AsgnStopOrd"]]
                    sc30_selected_gdf.loc[index,"AsgnStat"] = 1
    #                plt.text(r["BOYLAM"],r["ENLEM"],r["ARAC_NO"],fontsize=12,color="gray")
    #                count += 1
    #                if count > 20:
    #                    if (sum_distance/21.0).min() < 100:
    #                        for ind,row in group.iterrows():
    #                            indexForMin = sum_distance.idxmin()
    #                            sc30_selected_gdf.loc[ind,"AsgnStopOrd"] = stop_info.loc[indexForMin,"DURAK_SIRASI"]
    #                            group.loc[ind,"AsgnStopOrd"] = stop_info.loc[indexForMin,"DURAK_SIRASI"]
    #                        print(str(group.iloc[0]["AsgnStopOrd"])+" : "+str(group.iloc[0]["ARAC_NO"]) )
    #                    break
                    #sc30_selected_gdf.loc[j,"AsgnStopOrd"] = 
            if not os.path.exists(path+folder_name+"ULID"+lid+slid):
                os.makedirs(path+folder_name+"ULID"+lid+slid)
            sc30_selected_gdf.to_csv(path+folder_name+"ULID"+lid+slid+"/ULID"+lid+slid+"_"+str(day+1)+"oct.csv")
            sc30_selected_gdf.to_excel(path+folder_name+"ULID"+lid+slid+"/ULID"+lid+slid+"_"+str(day+1)+"oct.xlsx")
    
    
    stop_info_nonstationary = roadForPlot.loc[27:].to_crs(epsg=3857)
    stop_info_nonstationary["buffer_geo50m"] = stop_info_nonstationary["geometry"].buffer(50)
    stop_info_nonstationary["point"] = stop_info_nonstationary["geometry"]
    stop_info_nonstationary["geometry"] = stop_info_nonstationary["buffer_geo50m"]
    ord2ID_nonstationary = dict(zip(stop_info_nonstationary.DURAK_SIRASI, stop_info_nonstationary.DURAK_NO))
    for day in range(30):
        try:
            sc30 = pd.read_csv(path+"RawSCData/"+year+"/"+month+"/sc"+str(day+1)+".csv", encoding="ISO-8859-1")
        except FileNotFoundError:
            print("sc"+str(day+1)+".csv file does not exist!")
            continue
        for i in hatlar:
            lid = str(i)[0:-1]
            slid = str(i)[-1]
            
            sc30_selected =  sc30[(sc30["HAT_NO"]==10)&(sc30["ALT_HAT_NO"]==0)|(sc30["HAT_NO"]==10)&(sc30["ALT_HAT_NO"]==2)]
            sc30_selected_gdf = gpd.GeoDataFrame(sc30_selected,crs={'init':'epsg:4326'},geometry=[Point(x, y) for x,y in zip(sc30_selected.BOYLAM, sc30_selected.ENLEM)] )
            sc30_selected_gdf = sc30_selected_gdf.to_crs(epsg=3857)
            sc30_selected_gdf["ARAC_NO"] = sc30_selected_gdf["ARAC_NO"].astype(int)
            sc30_selected_gdf = sc30_selected_gdf[sc30_selected_gdf["ARAC_NO"]>=100]
            sc30_selected_gdf["Date"] = ""
            sc30_selected_gdf["Time"] = ""
            sc30_selected_gdf["ULID_corr"] = ""
            sc30_selected_gdf["ShiftID"] = ""
            sc30_selected_gdf["STripID"] = 0
            sc30_selected_gdf["DULTripID"] = 0
            for j, group in sc30_selected_gdf.groupby(by=["ARAC_NO"]):
    #            group.sort_values(by="TIMESTAMP",inplace=True)
    #            plt.scatter(group["BOYLAM"],group["ENLEM"],c="yellow")
    #            sum_distance = np.zeros((56-27+1))
    #            count = 0
                for index,r in group.iterrows():
    #                sum_distance += stop_info.geometry.distance(r.geometry)
                    sc30_selected_gdf.loc[index, "Date"] = sc30_selected_gdf.loc[index,"TIMESTAMP"].split(" ")[0]
                    sc30_selected_gdf.loc[index, "Time"] = sc30_selected_gdf.loc[index,"TIMESTAMP"].split(" ")[1]
                    
                    if j == 65 or j >= 100:
                        sc30_selected_gdf.loc[index, "ULID_corr"] = "ULID102"
                        sc30_selected_gdf.loc[index, "ShiftID"] = "102_"+str(j)
                    elif j < 100:
                        sc30_selected_gdf.loc[index, "ULID_corr"] = "ULID100"
                        sc30_selected_gdf.loc[index, "ShiftID"] = "100_"+str(j)
                    
                    for ind_stop, stop in stop_info_nonstationary.iterrows():
                        if stop["geometry"].contains(r["geometry"]):
                             sc30_selected_gdf.loc[index,"AsgnStopOrd"] = stop["DURAK_SIRASI"]
                             sc30_selected_gdf.loc[index,"AsgnStopID"] = stop["DURAK_ID"]
                             sc30_selected_gdf.loc[index,"AsgnStat"] = 1
                             break
                        else:
                             sc30_selected_gdf.loc[index,"AsgnStat"] = 0
    #                sc30_selected_gdf.loc[index,"AsgnStopOrd"] = arac_no_durak_dict[j]
    #                sc30_selected_gdf.loc[index,"AsgnStopID"] = ord2ID_nonstationary[sc30_selected_gdf.loc[index,"AsgnStopOrd"]]
    #                sc30_selected_gdf.loc[index,"AsgnStat"] = 1
                             
    #                plt.text(r["BOYLAM"],r["ENLEM"],r["ARAC_NO"],fontsize=12,color="gray")
    #                count += 1
    #                if count > 20:
    #                    if (sum_distance/21.0).min() < 100:
    #                        for ind,row in group.iterrows():
    #                            indexForMin = sum_distance.idxmin()
    #                            sc30_selected_gdf.loc[ind,"AsgnStopOrd"] = stop_info.loc[indexForMin,"DURAK_SIRASI"]
    #                            group.loc[ind,"AsgnStopOrd"] = stop_info.loc[indexForMin,"DURAK_SIRASI"]
    #                        print(str(group.iloc[0]["AsgnStopOrd"])+" : "+str(group.iloc[0]["ARAC_NO"]) )
    #                    break
                    #sc30_selected_gdf.loc[j,"AsgnStopOrd"] = 
            if not os.path.exists(path+folder_name+"1ULID"+lid+slid):
                os.makedirs(path+folder_name+"1ULID"+lid+slid)
            sc30_selected_gdf.to_csv(path+folder_name+"1ULID"+lid+slid+"/ULID"+lid+slid+"_"+str(day+1)+"oct.csv")
            sc30_selected_gdf.to_excel(path+folder_name+"1ULID"+lid+slid+"/ULID"+lid+slid+"_"+str(day+1)+"oct.xlsx")            
    #   sc30_selected1 = sc30[(sc30["HAT_NO"]==10)&(sc30["ALT_HAT_NO"]==0)|(sc30["HAT_NO"]==10)&(sc30["ALT_HAT_NO"]==2)]              
    # =============================================================================
    #                To check vid moving or not
    #                 if basePoint.geometry.distance(row.geometry) >= 400:
    #                     #print("arac_no: "+str(row["ARAC_NO"]))
    #                     print(basePoint.geometry.distance(row.geometry))
    #                     notGood1.append(row["ARAC_NO"])
    #                     break
    # =============================================================================
    # Combining tram data
    directory = path + folder_name
    files1 = [file[2] for file in os.walk(directory)][2]
    files1 = [file for file in files1 if file[-4:]==".csv"]
    files2 = [file[2] for file in os.walk(directory)][1]
    files2 = [file for file in files2 if file[-4:]==".csv"]
    
    lid = "10"
    slid = "0"
    for file in files1:
        df1 = pd.read_csv(directory + "ULID100/"+ file, encoding="ISO-8859-1")
        df2 = pd.read_csv(directory + "1ULID100/"+ file, encoding="ISO-8859-1")
        col_list = list(df1.columns)
        if len(df2) > 0:
            df2 = df2[col_list]
            df = df1.append(df2)
        else:
            df = df1.copy()
        if not os.path.exists(path+"SmartCardResults_"+year+"_post/"+month+"/ULID"+lid+slid):
            os.makedirs(path+"SmartCardResults_"+year+"_post/"+month+"/ULID"+lid+slid)
        df.to_csv(path+"SmartCardResults_"+year+"_post/"+month+"/ULID"+lid+slid+"/"+file)
        df.to_excel(path+"SmartCardResults_"+year+"_post/"+month+"/ULID"+lid+slid+"/"+file[:-3]+"xlsx")
    
