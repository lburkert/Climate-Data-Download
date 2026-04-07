###############################################################
#                                                             #
#        Helper function to convert df to NetCDF-file         #
#                                                             #
###############################################################



write_station_netcdf = function(df, sid, station_sf, outfile) {
  
  stations_in_bbox = station_sf
  out_file_nc <- outfile
  
  #NA to -9999.0
  vars <- c("tl","rf","ff","cglo","sh","rr")
  
  df_nc <- df
  
  # °C to Kelvin
  df_nc$tl + 273.15
  
  for(v in vars){
    df_nc[[v]][is.na(df_nc[[v]])] <- -9999.0
  }
  
  #convert time to seconds since 1970
  time_vals <- as.numeric(df_nc$time)
  
  timedim <- ncdim_def("time", "seconds since 1970-01-01 00:00:00", time_vals, unlim = TRUE)
  
 # get coordinates in 4326 for location of the station
  coords_4326 <- stations_in_bbox |>
    filter(Stations_id == sid) |>
    st_transform(4326) |>
    st_coordinates()
  
  lon_val <- coords_4326[1]
  lat_val <- coords_4326[2]
  
  londim <- ncdim_def("lon", "degrees_east", lon_val)
  latdim <- ncdim_def("lat", "degrees_north", lat_val)
  
  # helper function to create 3d array for time, lon, lat
  to_array <- function(x) {
    array(as.double(x), dim = c(1, 1, length(x)))
  }
  
  fill <- -9999.0
  
  # define variables
  
  var_temp <- ncvar_def("temp", "K", list(londim, latdim, timedim), fill, longname = "Air temperature 2m", prec = "double")
  var_precip <- ncvar_def("precip", "mm", list(londim, latdim, timedim), fill, longname = "Precipitation", prec = "double")
  var_swin <- ncvar_def("swin", "W m-2", list(londim, latdim, timedim), fill, longname = "Global radiation", prec = "double")
  var_hum <- ncvar_def("hum", "%", list(londim, latdim, timedim), fill, longname = "Relative humidity", prec = "double")
  var_ws <- ncvar_def("ws", "m/s", list(londim, latdim, timedim), fill, longname = "Wind speed", prec = "double")
  var_sh <- ncvar_def("sh", "m", list(londim, latdim, timedim), fill, longname = "Snow height", prec = "double")
  
  # create file
  nc <- nc_create(out_file_nc, list(var_temp, var_precip, var_swin, var_hum, var_ws, var_sh))
  
  # write coordinates
  ncvar_put(nc, "lon", lon_val)
  ncvar_put(nc, "lat", lat_val)
  ncvar_put(nc, "time", time_vals)
  
  ncatt_put(nc, "time", "calendar", "standard")
  ncatt_put(nc, "time", "long_name", "time")
  
  # write variables
  ncvar_put(nc, "temp",   to_array(df_nc$tl)) 
  ncvar_put(nc, "precip", to_array(df_nc$rr))
  ncvar_put(nc, "swin",   to_array(df_nc$cglo))
  ncvar_put(nc, "hum",    to_array(df_nc$rf))
  ncvar_put(nc, "ws",     to_array(df_nc$ff))
  ncvar_put(nc, "sh",     to_array(df_nc$sh))
  
  # put global attribute
  ncatt_put(nc, 0, "title", paste("Hourly climate data"))
  
  nc_close(nc)
}