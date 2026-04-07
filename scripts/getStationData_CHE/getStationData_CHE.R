###################################################################
#                                                                 #
#         SCRIPT TO DOWNLOAD Station Data FROM Meteo Swiss        #
#                                                                 #
###################################################################

# Source functions
source("scripts/getStationData_CHE/MeteoSwissAccessFunctions.R")
source("scripts/df_to_netcdf.R")

getStationData_CHE <- function(bbox, start_date, end_date, netcdf_output) {

  start_date <- as.POSIXct(start_date, tz = "UTC")
  end_date   <- as.POSIXct(end_date,   tz = "UTC")
  
  # get stations and hourly filenames for bbox from accessfunctions
  stations_in_bbox = getMeteoSwissStations(bbox)
  
  # exit if no stations found
  if (nrow(stations_in_bbox) == 0) {
    message("→ No Meteo-Swiss stations found in Bounding Box")
    return(invisible(NULL))
  }
  
  message("→ ", nrow(stations_in_bbox), " stations found")
  
  # create output directories (after station coverage check to avoid empty directories)
  out_dir <- "output/CHE_meteoswiss_station_data/"
  out_dir_merged <- "output/CHE_meteoswiss_station_data/merged/"
  out_dir_yearly <- file.path(out_dir, "yearly")
  dir.create(out_dir, showWarnings = FALSE)
  dir.create(out_dir_merged, showWarnings = FALSE)
  dir.create(out_dir_yearly, showWarnings = FALSE)
  
  # get start and end year for filtering
  start_year <- as.integer(format(start_date, "%Y"))
  end_year   <- as.integer(format(end_date, "%Y"))
  
  # csv files are downloaded for every 10 year timespan, so the relevant filepaths need to be found before the download 
  filter_assets_by_date <- function(asset_vec, start_year, end_year) {
    
    file_names <- names(asset_vec)
    
    # extract years from filename e.g. ...2010-2019...
    years <- regmatches(
      file_names,
      regexec("([0-9]{4})-([0-9]{4})", file_names)
    )
    
    years <- do.call(rbind, lapply(years, `[`, 2:3))
    years <- apply(years, 2, as.numeric)
    
    file_start <- years[,1]
    file_end   <- years[,2]
    
    # check if this file is relevant for the desired timeframe
    keep <- file_end >= start_year & file_start <= end_year
    
    asset_vec[keep]
  }
  
  # meteo swiss csv-files contain a lot of variables, we only keep a few. If others are requiered this should be changed here
  vars_keep <- c(
    "reference_timestamp",  # time
    "tre200h0",   # temperature
    "rre150h0",   # precipitation
    "gre000h0",   # global radiation
    "ure200h0",   # rel. humidity
    "fkl010h0",   # windspeed
    "htoauths"    # snow height
  )
  
  # function to read directly from the server without permanent download
  read_station_data <- function(asset_vec) {
    
    if (length(asset_vec) == 0)
      return(NULL)
    
    message("→ Reading ", asset_vec, " files")
    
    dfs <- lapply(unname(asset_vec), function(url) {
      
      # read csv as datatable from server
      dt <- fread(url, sep = ";", select = vars_keep, encoding = "Latin-1", na.strings = "", showProgress = FALSE)
      
      # parse datetime
      if ("time" %in% names(dt)) {
        dt[, time := as.POSIXct(time, format = "%d.%m.%Y %H:%M", tz = "UTC")]
      }
      
      dt
    })
    
    rbindlist(dfs, use.names = TRUE, fill = TRUE)
  }
  
  # create dataframe with every stations datatable
  station_dfs <- lapply(seq_len(nrow(stations_in_bbox)), function(i) {
    
    assets <- stations_in_bbox$assets[[i]]
    
    # filter for timeframe
    assets_filtered <- filter_assets_by_date(assets, start_year, end_year)
    
    # load data
    df_station <- read_station_data(assets_filtered)
    
    if (!is.null(df_station)) {
      df_station$station_id <- stations_in_bbox$id[i]
      df_station$station_name <- stations_in_bbox$title[i]
    }
    
    df_station
  })
  
  # filters for the exact start_date and end_date
  station_dfs_filtered <- lapply(station_dfs, function(df) {
    
    if (is.null(df) || nrow(df) == 0)
      return(NULL)
    
    # convert time column from string to POSIXct
    df[, reference_timestamp := as.POSIXct(reference_timestamp, format = "%d.%m.%Y %H:%M", tz = "UTC")]
    
    # filter time between start_date and end_date
    df <- df[reference_timestamp >= start_date & reference_timestamp <= end_date]
    
    df
  })
  
  # define colnames from austrian naming convention
  cols_needed <- c("time","station","tl","rr","cglo","rf","ff","sh")
  
  # change id column for netcdf function
  stations_in_bbox <- stations_in_bbox |> rename(Stations_id = id)
  
  # write file for each station
  for (station in station_dfs_filtered) {
    
    # skip if is null
    if (is.null(station) || nrow(station) == 0)
      next
    
    # store station name for the filename
    station_name <- unique(station$station_name)
    
    # remove station name from datatable
    station[,station_name:=NULL]
    
    # ---- rename columns (data.table way) ----
    setnames(station, old = c("reference_timestamp","tre200h0","ure200h0","fkl010h0","gre000h0","htoauths",
                              "rre150h0","station_id"),
                      new = c("time","tl","rf","ff","cglo","sh","rr","station"),
                      skip_absent = TRUE)
    
    # change the order of the columns to match geosphere order
    station = station |> select(all_of(cols_needed))
    
    #create file path
    out_file <- paste0(out_dir_merged, "station_", station_name,
                       "_", start_date, "_", end_date,".csv")
    
    #write file
    fwrite(station, out_file, na = "NA")
    
    #### optional NetCDF export ####
    if (netcdf_output) {
      
      out_dir_netcdf <- file.path(out_dir, "netcdf_merged")
      dir.create(out_dir_netcdf, showWarnings = FALSE)
      
      out_file_nc <- file.path(out_dir_netcdf, paste0("CHE_", station_name, "_", start_date, "_", end_date, ".nc"))
      
      write_station_netcdf(station, unique(station$station), stations_in_bbox, out_file_nc)
    }
    
    #### create yearly folders and files ####
    
    df <- as.data.frame(station)
    
    df_yearly <- df |>
      mutate(year = year(time))
    
    years <- unique(df_yearly$year)
    
    for (yr in years) {
      
      year_folder <- file.path(out_dir_yearly, yr)
      dir.create(year_folder, showWarnings = FALSE, recursive = TRUE)
      
      df_y <- df_yearly |>
        filter(year == yr) |>
        select(-year)
      
      out_file_year <- paste0(year_folder, "/", "station_", station_name, "_", yr, ".csv")
      
      write_csv(df_y, out_file_year)
      
      #### optional NetCDF export ####
      if (netcdf_output) {
        
        out_dir_netcdf <- file.path(out_dir, "netcdf_yearly")
        dir.create(out_dir_netcdf, showWarnings = FALSE)
        
        year_folder_nc <- file.path(out_dir_netcdf, yr)
        dir.create(year_folder_nc, showWarnings = FALSE, recursive = TRUE)
        
        out_file_nc <- file.path(year_folder_nc, paste0("CHE_", station_name, "_", yr, ".nc"))
        
        write_station_netcdf(df_y, unique(station$station), stations_in_bbox, out_file_nc)
      }
    }
  }
  
  return(stations_in_bbox)
}
