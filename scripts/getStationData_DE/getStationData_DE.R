###################################################################
#                                                                 #
# SCRIPT TO DOWNLOAD Station Data FROM Deutscher Wetterdienst     #
#                                                                 #
###################################################################

# Source functions
source("scripts/getStationData_DE/DWDAccessFunctions.R")
source("scripts/df_to_netcdf.R")

getStationData_DE <- function(bbox, start_date, end_date, netcdf_output) {
  
  # convert dates to dateformat
  start_date <- as.POSIXct(start_date, tz = "UTC")
  end_date   <- as.POSIXct(end_date,   tz = "UTC")
  
  # get all stations that are available from DWD
  stationen_sf_3416 = getDWDStations()
  
  # filter for stations in bbox
  stations_in_bbox <- stationen_sf_3416 %>%
    filter(st_within(geometry, bbox, sparse = FALSE))
  
  # exit if no stations overlap with the bbox
  if (nrow(stations_in_bbox) == 0) {
    message("→ No DWD-Stations in Bounding Box")
    return(invisible(NULL))
  }
  
  message("→ ", nrow(stations_in_bbox), " Stations found")
  
  # create output directories
  out_dir <- "output/DE_dwd_station_data/"
  out_dir_merged <- "output/DE_dwd_station_data/merged/"
  out_dir_yearly <- file.path(out_dir, "yearly")
  dir.create(out_dir, showWarnings = FALSE)
  dir.create(out_dir_merged, showWarnings = FALSE)
  dir.create(out_dir_yearly, showWarnings = FALSE)
  
  # all the paths for the variables are set here (if new variables are added, this should be done here)
  DWD_VARS <- list(
    TU = list(
      base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical/",
      prefix   = "stundenwerte_TU",
      product_pattern = "^produkt_tu_stunde_.*\\.txt$",
      select_cols = c("STATIONS_ID", "MESS_DATUM", "TT_TU", "RF_TU")
    ),
    RR = list(
      base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/",
      prefix   = "stundenwerte_RR",
      product_pattern = "^produkt_rr_stunde_.*\\.txt$",
      select_cols = c("STATIONS_ID", "MESS_DATUM", "R1")
    ),
    FF = list(
      base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical/",
      prefix   = "stundenwerte_FF",
      product_pattern = "^produkt_ff_stunde_.*\\.txt$",
      select_cols = c("STATIONS_ID", "MESS_DATUM", "F")
    ),
    ST = list(
      base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/solar/",
      prefix   = "stundenwerte_ST",
      product_pattern = "^produkt_st_stunde_.*\\.txt$",
      select_cols = c("STATIONS_ID", "MESS_DATUM", "FG_LBERG")
    ),
    SH = list(
      base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/",
      prefix   = "tageswerte_KL",
      product_pattern = "^produkt_klima_tag_.*\\.txt$",
      select_cols = c("STATIONS_ID", "MESS_DATUM", "SHK_TAG")
    )
  )
  
  
  get_links <- function(base_url) {
    resp <- request(base_url) |> req_perform()
    doc  <- read_html(resp_body_string(resp))
    nodes <- xml_find_all(doc, ".//a")
    xml_attr(nodes, "href")
  }
  
  
  
  find_zip <- function(station_id, links, prefix) {
    
    sid <- sprintf("%05d", as.integer(station_id))
    
    if (prefix == "stundenwerte_ST") {
      hit <- links[str_detect(links, paste0("^", prefix, "_", sid, ".*\\.zip$"))]
    } else {
      hit <- links[str_detect(
        links,
        paste0("^", prefix, "_", sid, "_\\d{8}_\\d{8}_hist\\.zip$")
      )]
    }
    
    if (length(hit) != 1) return(NA)
    hit
  }
  
  # function to convert date from character to date format
  convert_mess_datum <- function(x) {
    as.POSIXct(
      as.character(x),
      format = "%Y%m%d%H",
      tz = "UTC"
    )
  }
  
  station_ids <- stations_in_bbox$Stations_id
  
  
  for (var in names(DWD_VARS)) {
    
    cat("\n=== Variable:", var, "===\n")
    
    cfg <- DWD_VARS[[var]]
    links <- get_links(cfg$base_url)
    
    for (sid_raw in station_ids) {
      
      sid <- sprintf("%05d", as.integer(sid_raw))
      cat("Station", sid, "\n")
      
      zip_name <- find_zip(sid_raw, links, cfg$prefix)
      
      if (is.na(zip_name)) {
        cat("  → No data available\n")
        next
      }
      
      zip_url  <- paste0(cfg$base_url, zip_name)
      zip_file <- file.path(out_dir, paste0(var, "_", sid, ".zip"))
      extract_dir <- file.path(out_dir, paste0("station_", sid))
      dir.create(extract_dir, showWarnings = FALSE)
      
      # Download the data
      resp <- request(zip_url) |> req_timeout(60) |> req_perform()
      if (resp_status(resp) != 200) {
        cat("  → Download failed\n")
        next
      }
      
      writeBin(resp_body_raw(resp), zip_file)
      unzip(zip_file, exdir = extract_dir)
      
      # find product file
      txt_file <- list.files(
        extract_dir,
        pattern = cfg$product_pattern,
        full.names = TRUE
      )
      
      if (length(txt_file) != 1) {
        cat("  → Product file not found\n")
        next
      }
      
      # read downloaded file as df
      df <- read_delim(
        txt_file,
        delim = ";",
        col_types = cols(),
        locale = locale(decimal_mark = "."),
        trim_ws = TRUE
      )
      
      # convert missing values from -999 to NA
      df <- df |> 
        mutate(across(where(is.numeric), ~na_if(., -999)))
      
      # convert date and updample snow height from daily to hourly
      # snow height is treated differently since it is the only variable that is only recorded daily
      if (var == "SH") {
        
        df <- df |>
          mutate(
            MESS_DATUM_DAY = as.Date(as.character(MESS_DATUM), format = "%Y%m%d")
          ) |>
          filter(
            MESS_DATUM_DAY >= as.Date(start_date),
            MESS_DATUM_DAY <= as.Date(end_date)
          )
        
        if (nrow(df) == 0) {
          cat("  → No data available in the requested timeframe\n")
          next
        }
        
        # 2. Upsampling: every daily value is split into 24 rows with the same value for snow height
        df <- df |>
          group_by(STATIONS_ID, MESS_DATUM_DAY) |>
          reframe(
            MESS_DATUM = seq(
              from = as.POSIXct(paste(MESS_DATUM_DAY, "00:00:00"), tz = "UTC"),
              by   = "hour",
              length.out = 24
            ),
            across(where(is.numeric), first)
          ) |>
          ungroup() |>
          select(-MESS_DATUM_DAY)
        
      } else {
        # now all the other variables are filtered for the timeframe TU, RR, FF, ST
        df <- df |>
          mutate(
            MESS_DATUM = convert_mess_datum(MESS_DATUM)
          ) |>
          filter(
            MESS_DATUM >= start_date,
            MESS_DATUM <= end_date
          )
      }
      
      # checks if data is available for the requested timeframe
      if (nrow(df) == 0) {
        cat("  → No data available in the requested timeframe\n")
        next
      }
      
      # selevt only the station id, the date and the requiered variables
      df_out <- df |> select(any_of(cfg$select_cols))
      
      # creates path for csv output
      out_csv <- file.path(
        extract_dir,
        paste0("station_", sid, "_", var, ".csv")
      )
      
      # creates csv for each station and variable
      write_csv(df_out, out_csv)
      cat("  → CSV saved to ", out_csv, "\n")
    }
  }
  
  # function to merge variables
  merge_station_vars <- function(station_id, base_dir) {
    
    sid <- sprintf("%05d", as.integer(station_id))
    station_dir <- file.path(base_dir, paste0("station_", sid))
    
    # temperature is used as the first var to create the df as it will most likely be available
    tu_file <- file.path(station_dir, paste0("station_", sid, "_TU.csv"))
    if (!file.exists(tu_file)) return(NULL)
    
    df_tu <- read_csv(tu_file, show_col_types = FALSE) |>
      distinct(STATIONS_ID, MESS_DATUM, .keep_all = TRUE)
    
    df_all <- df_tu
    
    # all the other vars
    other_vars <- c("RR", "FF", "ST", "SH")
    
    # adds the other vars to the df_all dataframe
    for (v in other_vars) {
      
      f <- file.path(station_dir, paste0("station_", sid, "_", v, ".csv"))
      if (!file.exists(f)) next
      
      # reads the coresponding variable csv
      df_v <- read_csv(f, show_col_types = FALSE) |>
        distinct(STATIONS_ID, MESS_DATUM, .keep_all = TRUE)
      
      # joins the variable column to the existing merged dataframe
      df_all <- left_join(
        df_all,
        df_v,
        by = c("STATIONS_ID", "MESS_DATUM")
      )
    }
    
    df_all
  }
  
  # applies the merging function
  for (sid in stations_in_bbox$Stations_id) {
    
    df <- merge_station_vars(sid, out_dir)
    if (is.null(df)) next
    
    station_name <- stations_in_bbox |>
      filter(Stations_id == sid) |>
      pull(Stationsname)
    
    # rename colomns to match austrian naming sequence
    cols_needed <- c("time","station","tl","rr","cglo","rf","ff","sh")
    
    # rename column names to match geosphere variable names
    df <- df |>
      rename(
        station = any_of("STATIONS_ID"),
        time    = any_of("MESS_DATUM"),
        tl      = any_of("TT_TU"),
        rf      = any_of("RF_TU"),
        ff      = any_of("F"),
        cglo    = any_of("FG_LBERG"),
        sh      = any_of("SHK_TAG"),
        rr      = any_of("R1")
      )
    
    # create NA columns for missing variables so all files have the same structure and dims 
    missing_cols <- setdiff(cols_needed, names(df))
    df[missing_cols] <- NA
    
    # change the order of the columns to match geosphere order
    df <- df |> select(all_of(cols_needed))
    

    #create file path
    out_file <- paste0(out_dir_merged, "station_", sprintf("%05d", as.integer(sid)), "_", station_name,
                       "_", start_date, "_", end_date,".csv")

    #write file
    write_csv(df, out_file)
    
    #### optional NetCDF export ####
    if (netcdf_output) {
      
      out_dir_netcdf <- file.path(out_dir, "netcdf_merged")
      dir.create(out_dir_netcdf, showWarnings = FALSE)
      
      out_file_nc <- file.path(out_dir_netcdf, paste0("DE_", sprintf("%05d", as.integer(sid)), "_", station_name, "_", start_date, "_", end_date, ".nc"))
      
      write_station_netcdf(df, sid, stations_in_bbox, out_file_nc)
    }
    
    #### create yearly folders and files ####
    df_yearly <- df |>
      mutate(year = year(time))
    
    years <- unique(df_yearly$year)
    
    for (yr in years) {
      
      year_folder <- file.path(out_dir_yearly, yr)
      dir.create(year_folder, showWarnings = FALSE, recursive = TRUE)
      
      df_y <- df_yearly |>
        filter(year == yr) |>
        select(-year)
      
      out_file_year <- paste0(year_folder, "/", "station_", sprintf("%05d", as.integer(sid)), "_", station_name, "_", yr, ".csv")
      
      write_csv(df_y, out_file_year)
      
      #### optional NetCDF export ####
      if (netcdf_output) {
        
        out_dir_netcdf <- file.path(out_dir, "netcdf_yearly")
        dir.create(out_dir_netcdf, showWarnings = FALSE)
        
        year_folder_nc <- file.path(out_dir_netcdf, yr)
        dir.create(year_folder_nc, showWarnings = FALSE, recursive = TRUE)
        
        out_file_nc <- file.path(year_folder_nc, paste0("DE_", sprintf("%05d", as.integer(sid)), "_", station_name, "_", yr, ".nc"))
        
        write_station_netcdf(df_y, sid, stations_in_bbox, out_file_nc)
      }
    }
    
  }
  
  # select raw unmerged station data for deletion
  files <- list.files(out_dir, full.names = TRUE)
  files_to_delete <- files[!basename(files) %in% c("merged", "yearly", "netcdf_merged", "netcdf_yearly")]
  
  # delete files that are no longer needed
  unlink(files_to_delete, recursive = TRUE)
  
  return(stations_in_bbox)
}


