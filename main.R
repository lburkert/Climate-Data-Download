###################################################################
#                                                                 #
# MAIN SCRIPT: Steuerung für Klimadaten-Download                  #
#                                                                 #
###################################################################

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


# Set working directory auf den Script-Ordner
script_path = this.path()
setwd(dirname(script_path))

# ------------------
# Parameter
# ------------------

# Set start and end date
start_date <- as.Date("2015-01-01")
end_date   <- as.Date("2020-12-31")

#Read bounding box
# Format: xmin, xmax, ymin, ymax, crs
bbox_3416 <- c(
  xmin = 220000,  # westliche Grenze
  xmax = 244000,  # östliche Grenze
  ymin = 386000,  # südliche Grenze
  ymax = 398000   # nördliche Grenze
)

# Buffer [m] to extend bounding box (might yield more stations)
xbuffer=7000
ybuffer=7000

# ---------------------------------
# PREPARATORY STEPS
# ---------------------------------

# Modify AOI using the buffer
# Added: correct buffer application to bbox elements (xmin/xmax/ymin/ymax)
bbox_3416["xmin"] <- bbox_3416["xmin"] - xbuffer
bbox_3416["xmax"] <- bbox_3416["xmax"] + xbuffer
bbox_3416["ymin"] <- bbox_3416["ymin"] - ybuffer
bbox_3416["ymax"] <- bbox_3416["ymax"] + ybuffer


bbox_poly_3416 <- st_as_sfc(st_bbox(bbox_3416, crs = 3416))

# ------------------
# Scripts laden
# ------------------
source("scripts/GetStationDataGeosphere_new/GetStationDataGeosphere.R")
source("scripts/getStationData_DE/getStationData_DE.R")

# ------------------
# Ausführen
# ------------------
stations_AT = getStationData_AT(bbox_poly_3416, start_date, end_date)
stations_DE = getStationData_DE(bbox_poly_3416, start_date, end_date)

plot(bbox_poly_3416, col = NA, border = "black", lwd = 2)
plot(stations_DE, add = TRUE, col = "green", pch = 16)
plot(stations_AT, add = TRUE, col = "red", pch = 16)

coords_DE <- st_coordinates(stations_DE)
coords_AT <- st_coordinates(stations_AT)

# Labels hinzufügen
text(coords_DE[,1], coords_DE[,2],
     labels = stations_DE$Stationsname,   # <- Spaltenname anpassen!
     pos = 4,                     # rechts vom Punkt
     cex = 0.6,
     col = "darkgreen")

text(coords_AT[,1], coords_AT[,2],
     labels = stations_AT$Stationsname,
     pos = 4,
     cex = 0.6,
     col = "darkred")


