###############################################################
#                                                             #
# SCRIPT TO DOWNLOAD STATION DATA FROM Deutscher Wetterdienst #
#                                                             #
###############################################################

# Required packages:
# sf

getDWDStations = function(){
  
  # 1. Define the URL
  url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical/TU_Stundenwerte_Beschreibung_Stationen.txt"
  
  # 2. Download and read the data
  col_names <- c(
    "Stations_id", "von_datum", "bis_datum",
    "Stationshoehe", "geoBreite", "geoLaenge",
    "Stationsname", "Bundesland", "Abgabe"
  )
  
  widths <- c(5, 9, 9, 15, 12, 10, 41, 41, 6)
  
  # Datei einlesen
  stationen <- read_fwf(
    file = url,
    fwf_widths(widths, col_names = col_names),
    skip = 2, 
    locale = locale(encoding = "ISO-8859-1"), 
    trim_ws = TRUE,
    show_col_types = FALSE
  )
  
  stationen_sf <- st_as_sf(
    stationen,
    coords = c("geoLaenge", "geoBreite"),
    crs = 4326,
    remove = FALSE                          
  )
  
  stationen_sf_3416 <- st_transform(stationen_sf, 3416)
  
  return(stationen_sf_3416)
}