###########################################################
#                                                         #
# SCRIPT TO DOWNLOAD STATION DATA FROM GEOSPHERE DATA HUB #
#                                                         #
###########################################################


# Source functions
source("scripts/GetStationDataGeosphere_new/DataHubAccessFunctions.R")

############################################################
# CONFIG - MODIFY ONLY THIS SECTION
############################################################

getStationData_AT <- function(bbox, start_date, end_date) {
  
  start_date <- as.POSIXct(start_date, tz = "UTC")
  end_date   <- as.POSIXct(end_date,   tz = "UTC")
  
  # Dataset + parameters + period
  resource_id  <- "klima-v2-1h"
  mode         <- "historical"
  parameters   <- c("TL","RR","CGLO","RF","FF","SH")
  
  # Output directory
  out_dir <- "output/AT_klima_1h_by_station"
  
  ############################################################
  # PREPARATORY STEPS
  ############################################################
  
  # Create output directory
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Added: keep both yearly and merged outputs
  yearly_dir <- file.path(out_dir, "yearly")
  merged_dir <- file.path(out_dir, "merged")
  dir.create(yearly_dir, showWarnings = FALSE, recursive = TRUE)
  dir.create(merged_dir, showWarnings = FALSE, recursive = TRUE)
  
  ############################################################
  # 1) Get station metadata and convert to EPSG:3416
  ############################################################
  
  stations <- get_metadata(
    type        = "station",
    mode        = mode,
    resource_id = resource_id,
    section     = "stations"
  )
  
  # Adjust these if your metadata uses different column names:
  lat_col <- "Breite [°N]"
  lon_col <- "Länge [°E]"
  id_col  <- "id"
  
  if (!all(c(lat_col, lon_col, id_col) %in% names(stations))) {
    stop("Check station metadata column names and update lat_col / lon_col / id_col.")
  }
  
  stations_sf <- st_as_sf(
    stations,
    coords = c(lon_col, lat_col),
    crs    = 4326    # station metadata is usually WGS84
  )
  
  stations_3416 <- st_transform(stations_sf, 3416)
  
  ############################################################
  # 2) Build bbox polygon in EPSG:3416 and select stations
  ############################################################
  
  inside <- st_within(stations_3416, bbox_poly_3416, sparse = FALSE)[, 1]
  stations_in_box <- stations_3416[inside, ]
  
  if (nrow(stations_in_box) == 0) {
    stop("No stations in austria found inside the given EPSG:3416 bounding box.")
  }
  
  station_ids <- as.character(stations_in_box[[id_col]])
  
  message("Found ", length(station_ids), " station(s) in bbox: ",
          paste(station_ids, collapse = ", "))
  
  # Added: station info lookup (coverage + name)
  station_info <- data.frame(
    id    = as.character(stations_in_box[[id_col]]),
    name  = as.character(stations_in_box[["Stationsname"]]),
    start = as.Date(stations_in_box[["Startdatum"]]),
    end   = as.Date(stations_in_box[["Enddatum"]]),
    stringsAsFactors = FALSE
  )
  
  # Added: helper to get station name safely
  get_station_name <- function(sid) {
    x <- station_info$name[match(sid, station_info$id)]
    if (length(x) == 0 || is.na(x) || !nzchar(x)) return("NA")
    x
  }
  
  # Added: make station names safe for filenames (no spaces, slashes, umlauts etc.)
  make_safe_name <- function(x) {
    x <- iconv(x, to = "ASCII//TRANSLIT")        # transliterate umlauts etc.
    x <- gsub("[^A-Za-z0-9]+", "-", x)          # replace any non-alphanumeric with "-"
    x <- gsub("-+", "-", x)                     # collapse multiple "-"
    x <- gsub("^-|-$", "", x)                   # trim leading/trailing "-"
    if (!nzchar(x)) x <- "NA"
    x
  }
  
  ############################################################
  # 3) Loop over stations, download & save CSV per station
  ############################################################
  
  # Added: expected column names based on your example file
  time_col <- "time"
  sid_col  <- "station"
  
  # Added: append CSV safely without rereading large files
  append_csv <- function(df, path) {
    dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
    if (!file.exists(path)) {
      readr::write_csv(df, path)
    } else {
      # Added: append without header (keeps existing file)
      readr::write_csv(df, path, append = TRUE, col_names = FALSE)
    }
  }
  
  # Added: parse time stamps robustly (works with ISO-like strings)
  parse_time_utc <- function(x) {
    # Try direct parsing; if it fails, return NA
    suppressWarnings(as.POSIXct(x, tz = "UTC"))
  }
  
  # Added: update yearly files from newly downloaded rows only
  update_yearly_files <- function(df_new, station_id) {
    if (!all(c(time_col, sid_col) %in% names(df_new))) return(invisible(NULL))
    
    tt <- parse_time_utc(df_new[[time_col]])
    if (all(is.na(tt))) return(invisible(NULL))
    
    years <- format(tt, "%Y")
    df_new$.__year <- years
    
    split_list <- split(df_new, df_new$.__year)
    for (yy in names(split_list)) {
      year_path <- file.path(yearly_dir, yy)
      dir.create(year_path, showWarnings = FALSE, recursive = TRUE)
      
      # Added: include station name in yearly filenames: name_id_period
      sname <- get_station_name(station_id)
      safe_name <- make_safe_name(sname)
      
      f_year <- file.path(year_path, paste0(resource_id, "_", safe_name, "_", station_id, "_", yy, ".csv"))
      df_y_new <- split_list[[yy]]
      df_y_new$.__year <- NULL
      
      # Added: keep yearly files clean (append + dedupe by time/station + overwrite)
      if (file.exists(f_year)) {
        df_old <- tryCatch(readr::read_csv(f_year, show_col_types = FALSE), error = function(e) NULL)
        if (!is.null(df_old) && nrow(df_old) > 0) {
          df_all <- rbind(df_old, df_y_new)
        } else {
          df_all <- df_y_new
        }
      } else {
        df_all <- df_y_new
      }
      
      df_all[[time_col]] <- as.character(df_all[[time_col]])
      df_all[[sid_col]]  <- as.character(df_all[[sid_col]])
      df_all <- df_all[!duplicated(df_all[, c(time_col, sid_col)]), , drop = FALSE]
      
      tt_all <- parse_time_utc(df_all[[time_col]])
      df_all <- df_all[order(tt_all), , drop = FALSE]
      
      readr::write_csv(df_all, f_year)
    }
    
    invisible(NULL)
  }
  
  requested_start <- as.Date(start_date)
  requested_end   <- as.Date(end_date)
  
  for (sid in station_ids) {
    
    sname <- get_station_name(sid)
    message("Processing station ", sid, " (", sname, ") ...")
    
    cov_idx <- match(sid, station_info$id)
    station_start <- station_info$start[cov_idx]
    station_end   <- station_info$end[cov_idx]
    
    effective_start <- max(requested_start, station_start, na.rm = TRUE)
    effective_end   <- min(requested_end, station_end, na.rm = TRUE)
    
    # Added: skip if no overlap with the requested period
    if (is.na(effective_start) || is.na(effective_end) || effective_start > effective_end) {
      message("  ⚠️ No overlap with requested period (skipping).")
      next
    }
    
    # Added: include station name in merged filename: name_id_period (canonical file per station)
    safe_name <- make_safe_name(sname)
    
    merged_file <- file.path(
      merged_dir,
      paste0(resource_id, "_", safe_name, "_", sid, "_", start_date, "_", end_date, ".csv")
    )
    
    # Added: incremental download - if merged file exists, continue after last timestamp
    if (file.exists(merged_file)) {
      last_time <- tryCatch({
        # Read only the time column to find the last timestamp
        df_old_time <- readr::read_csv(merged_file, col_select = all_of(time_col), show_col_types = FALSE)
        tt <- parse_time_utc(df_old_time[[time_col]])
        max(tt, na.rm = TRUE)
      }, error = function(e) NA)
      
      if (!is.na(last_time)) {
        # klima-v2-1h -> step is 1 hour; add one hour to avoid duplicates
        next_start <- as.Date(last_time + 3600)
        effective_start <- max(effective_start, next_start, na.rm = TRUE)
      }
    }
    
    # Added: after incremental shift, check overlap again
    if (is.na(effective_start) || effective_start > effective_end) {
      message("  ✅ Up to date (nothing new to download).")
      next
    }
    
    message("  Download window: ", as.character(effective_start), " to ", as.character(effective_end))
    
    df_new <- tryCatch({
      download_station_data(
        mode          = mode,
        resource_id   = resource_id,
        parameters    = parameters,
        station_ids   = sid,
        start         = as.character(effective_start),
        end           = as.character(effective_end),
        output_format = "csv",
        out_file      = NULL,
        verbose       = FALSE
      )
    }, error = function(e) {
      message("  ❌ Download failed for station ", sid, " (", sname, "): ", conditionMessage(e))
      NULL
    })
    
    if (is.null(df_new) || nrow(df_new) == 0) {
      message("  ⚠️ No new data returned.")
      next
    }
    
    # Added: sanity check for expected columns
    if (!all(c(time_col, sid_col) %in% names(df_new))) {
      message("  ❌ Missing expected columns in returned data. Found: ", paste(names(df_new), collapse = ", "))
      next
    }
    
    # Added: write/append merged file
    append_csv(df_new, merged_file)
    message("  ✅ Updated merged: ", merged_file)
    
    # Added: update yearly files based only on the new rows (keeps both products)
    update_yearly_files(df_new, sid)
    message("  ✅ Updated yearly files for station ", sid, " (", sname, ")")
  }
  
  message("Done. Files written to: ", normalizePath(out_dir))
  return(stations_in_box)
}