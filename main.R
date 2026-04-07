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
#                                                                                                      #
########################################################################################################

# Clear environment
rm(list = ls())


library(this.path)
library(sf)
library(readr)
library(dplyr)
library(lubridate)
library(zip)
library(purrr)
library(httr2)
library(xml2)
library(stringr)
library(data.table)
library(ncdf4)
library(jsonlite)


# Set working directory auf den Script-Ordner
script_path = this.path()
setwd(dirname(script_path))

########################################################################################################
#                                                                                                      #
#                                                 INPUT                                                # 
#                                                                                                      #
########################################################################################################

# Set start and end date
start_date <- as.Date("2019-12-01")
end_date   <- as.Date("2020-12-31")

#Read bounding box
# Format: xmin, xmax, ymin, ymax, crs
bbox_3416 <- c(
  xmin = 164612,  # westliche Grenze
  xmax = 190139,  # östliche Grenze
  ymin = 327985,  # südliche Grenze
  ymax = 451429   # nördliche Grenze
)

# Buffer [m] to extend bounding box (might yield more stations)
xbuffer=7000
ybuffer=7000

# create NetCDF files
netcdf_output = TRUE

########################################################################################################
#                                                                                                      #
#                                               END OF INPUT                                           # 
#                                                                                                      #
########################################################################################################

# Buffer for bbox
bbox_3416["xmin"] <- bbox_3416["xmin"] - xbuffer
bbox_3416["xmax"] <- bbox_3416["xmax"] + xbuffer
bbox_3416["ymin"] <- bbox_3416["ymin"] - ybuffer
bbox_3416["ymax"] <- bbox_3416["ymax"] + ybuffer

# bbox to poly
bbox_poly_3416 <- st_as_sfc(st_bbox(bbox_3416, crs = 3416))

# ------------------
# load scricpts for every country from scripts folder
# ------------------
source("scripts/GetStationData_AT/GetStationDataGeosphere.R")
source("scripts/getStationData_DE/getStationData_DE.R")
source("scripts/getStationData_CHE/getStationData_CHE.R")
# download for italy not functional yet
#source("scripts/getStationData_IT/getStationData_IT.R")

# ------------------
# execute for every country and store for overview plot
# ------------------
stations_AT = getStationData_AT(bbox_poly_3416, start_date, end_date, netcdf_output)
stations_DE = getStationData_DE(bbox_poly_3416, start_date, end_date, netcdf_output)
stations_CHE = getStationData_CHE(bbox_poly_3416, start_date, end_date, netcdf_output)
#stations_IT = getStationData_DE(bbox_poly_3416, start_date, end_date, username, password)

plot(bbox_poly_3416, col = NA, border = "black", lwd = 2)

# plot available german stations
if (exists("stations_DE") && !is.null(stations_DE) && nrow(stations_DE) > 0) {
  plot(stations_DE, add = TRUE, col = "green", pch = 16)
  coords_DE <- st_coordinates(stations_DE)
  text(coords_DE[,1], coords_DE[,2],
       labels = stations_DE$Stationsname,
       pos = 4, cex = 0.6, col = "darkgreen")
}

# plot available austrian stations
if (exists("stations_AT") && !is.null(stations_AT) && nrow(stations_AT) > 0) {
  plot(stations_AT, add = TRUE, col = "red", pch = 16)
  coords_AT <- st_coordinates(stations_AT)
  text(coords_AT[,1], coords_AT[,2],
       labels = stations_AT$Stationsname,
       pos = 4, cex = 0.6, col = "darkred")
}

# plot available swiss stations
if (exists("stations_CHE") && !is.null(stations_CHE) && nrow(stations_CHE) > 0) {
  plot(stations_CHE, add = TRUE, col = "blue", pch = 16)
  coords_CHE <- st_coordinates(stations_CHE)
  text(coords_CHE[,1], coords_CHE[,2],
       labels = stations_CHE$title,
       pos = 4, cex = 0.6, col = "blue")
}

