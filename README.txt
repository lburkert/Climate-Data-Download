########################################################################################################
#                                                                                                      #
#                         MAIN SCRIPT: CONTROL SCRIPT FOR CLIMATE DATA-DOWNLOAD                        # 
#                                                                                                      #
########################################################################################################
#                                                                                                      #
#     This script automatically downloads hourly climate data for stations                             #
#     in Austria, Germany and Switzerland                                                              #
#                                                                                                      #
#     ------------------------------------------------------------------------------------------       #
#                                                                                                      #
#     INPUT: Bounding Box (four coordinates) in 3416                                                   #
#             Start Date                                                                               #
#             End Date                                                                                 #  
#                                                                                                      #
#     ------------------------------------------------------------------------------------------       #
#
#     OUTPUT: CSV files with hourly values for six variables:                                          #
#             temperature, precipitation, relative humidity, global radiation,                         #
#             windspeed and snow height are                                                            #
#                                                                                                      #
#             - files are saved for every station for the whole timespan, as well as seperated by year #  
#                                                                                                      #
#             - missing values are stored as NA, if the entire variable is not available at a          #
#             certain station, the entire column is also set to NA, file structure is always the same  #
#                                                                                                      #
#             - NetCDF files can optionally be saved when "netcdf" option is set to TRUE               #
#               										       #
#		! DWD Stations (Germany) only provide snow height on a daily basis. Snow height data ! #
# 		! from those stations was upscaled to match hourly data                              ! #                                                        #												       #
########################################################################################################

Example Folder structure for bbox_3416 <- c(xmin = 164612, xmax = 190139, ymin = 327985, ymax = 451429)

D:.
|   main.R
|   README.txt
|   
+---output
|   +---AT_klima_1h_by_station
|   |   +---merged
|   |   |       klima-v2-1h_Galtur_17002_2019-12-01_2020-12-31.csv
|   |   |       klima-v2-1h_Galtur_28_2019-12-01_2020-12-31.csv
|   |   |       ...
|   |   |       
|   |   +---netcdf_merged
|   |   |       AT_11305_Warth_2019-12-01_2020-12-31.nc
|   |   |       AT_11311_Mittelberg_2019-12-01_2020-12-31.nc
|   |   |       ...
|   |   |       
|   |   +---netcdf_yearly
|   |   |   +---2019
|   |   |   |       AT_11305_Warth_2019.nc
|   |   |   |       AT_11311_Mittelberg_2019.nc
|   |   |   |       ...
|   |   |   |       
|   |   |   \---2020
|   |   |           AT_11305_Warth_2020.nc
|   |   |           AT_11311_Mittelberg_2020.nc
|   |   |           ...
|   |   |           
|   |   \---yearly
|   |       +---2019
|   |       |       klima-v2-1h_Galtur_17002_2019.csv
|   |       |       klima-v2-1h_Galtur_28_2019.csv
|   |       |       ...
|   |       |       
|   |       \---2020
|   |               klima-v2-1h_Galtur_17002_2020.csv
|   |               klima-v2-1h_Galtur_28_2020.csv
|   |               ...
|   |               
|   +---CHE_meteoswiss_station_data
|   |   +---merged
|   |   |       station_Naluns-Schlivera-NAS_2019-12-01_2020-12-31.csv
|   |   |       station_Scuol-SCU_2019-12-01_2020-12-31.csv
|   |   |       
|   |   +---netcdf_merged
|   |   |       CHE_Naluns-Schlivera-NAS_2019-12-01_2020-12-31.nc
|   |   |       CHE_Scuol-SCU_2019-12-01_2020-12-31.nc
|   |   |       
|   |   +---netcdf_yearly
|   |   |   +---2019
|   |   |   |       CHE_Naluns-Schlivera-NAS_2019.nc
|   |   |   |       CHE_Scuol-SCU_2019.nc
|   |   |   |       
|   |   |   \---2020
|   |   |           CHE_Naluns-Schlivera-NAS_2020.nc
|   |   |           CHE_Scuol-SCU_2020.nc
|   |   |           
|   |   \---yearly
|   |       +---2019
|   |       |       station_Naluns-Schlivera-NAS_2019.csv
|   |       |       station_Scuol-SCU_2019.csv
|   |       |       
|   |       \---2020
|   |               station_Naluns-Schlivera-NAS_2020.csv
|   |               station_Scuol-SCU_2020.csv
|   |               
|   \---DE_dwd_station_data
|       +---merged
|       |       station_02559_Kempten_2019-12-01_2020-12-31.csv
|       |       station_03730_Oberstdorf_2019-12-01_2020-12-31.csv
|       |       ...
|       |       
|       +---netcdf_merged
|       |       DE_02559_Kempten_2019-12-01_2020-12-31.nc
|       |       DE_03730_Oberstdorf_2019-12-01_2020-12-31.nc
|       |       ...
|       |       
|       +---netcdf_yearly
|       |   +---2019
|       |   |       DE_02559_Kempten_2019.nc
|       |   |       DE_03730_Oberstdorf_2019.nc
|       |   |       ...
|       |   |       
|       |   \---2020
|       |           DE_02559_Kempten_2020.nc
|       |           DE_03730_Oberstdorf_2020.nc
|       |           ...
|       |           
|       \---yearly
|           +---2019
|           |       station_02559_Kempten_2019.csv
|           |       station_03730_Oberstdorf_2019.csv
|           |       ...
|           |       
|           \---2020
|                   station_02559_Kempten_2020.csv
|                   station_03730_Oberstdorf_2020.csv
|                   ...
|                   
\---scripts
    |   df_to_netcdf.R
    |   
    +---GetStationData_AT
    |       DataHubAccessFunctions.R
    |       GetStationDataGeosphere.R
    |       
    +---getStationData_CHE
    |       .RData
    |       .Rhistory
    |       getStationData_CHE.R
    |       MeteoSwissAccessFunctions.R
    |       
    +---getStationData_DE
    |       .RData
    |       .Rhistory
    |       DWDAccessFunctions.R
    |       getStationData_DE.R
    |       
    \---getStationData_IT
            getStationData_IT.R
            MeteoItalia_access_functions.R

####

Variablenbenennung Österreich

tl = Lufttemperatur 2m
rr = Niederschlag
cglo = Globalstrahlung Mittelwert
rf = Relative Feuchte
ff = Windgeschwindigkeit
sh = Gesamtschneehöhe, Schneepegelmessung

Variablenbenennung Deutschland

TT_TU = Lufttemperatur
RF_TU = relative Feuchte
R1 = Niederschlag
F = Windgeschwindigkeit
FG_LBERG = Stundensumme der Globalstrahlung
SHK_TAG = Tageswert Schneehöhe

Variablenbenennung Schweiz

reference_timestamp = time
tre200h0 = temperature
rre150h0 = precipitation
gre000h0 = global radiation
ure200h0 = rel. humidity
fkl010h0 = windspeed
htoauths = snow height