############################################################
# GeoSphere Austria Dataset API helper functions
# Docs: https://dataset.api.hub.geosphere.at/v1/docs/
#
# No authentication is required at the moment.
# Request limits (see API: Grundlagen / Request-Limits):
#   - Max 5 requests / second
#   - Max 240 requests / hour
#   - Size limit: 1,000,000 values for json/csv, 10,000,000 for NetCDF
############################################################

# Required packages --------------------------------------------------------

if (!requireNamespace("httr", quietly = TRUE)) {
  stop("Package 'httr' is required. Install it with install.packages('httr').")
}
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required. Install it with install.packages('jsonlite').")
}
if (!requireNamespace("readr", quietly = TRUE)) {
  stop("Package 'readr' is required. Install it with install.packages('readr').")
}

library(httr)
library(jsonlite)
library(readr)

# Base URL -----------------------------------------------------------------

GEOSPHERE_BASE_URL <- "https://dataset.api.hub.geosphere.at/v1"

# Added: default request robustness settings (retry/timeout)
GEOSPHERE_TIMEOUT_SECONDS <- 120
GEOSPHERE_RETRY_TIMES <- 5
GEOSPHERE_RETRY_PAUSE_MIN <- 1
GEOSPHERE_RETRY_PAUSE_CAP <- 10
# Added: do not retry on client errors that won’t succeed if repeated
GEOSPHERE_TERMINATE_ON <- c(400, 401, 403, 404)

# Added: include response body in HTTP errors to diagnose 400/413 etc.
stop_for_status_with_body <- function(resp) {
  if (httr::http_error(resp)) {
    status <- httr::status_code(resp)
    body_txt <- tryCatch(
      httr::content(resp, as = "text", encoding = "UTF-8"),
      error = function(e) ""
    )
    msg <- paste0("HTTP ", status, if (nzchar(body_txt)) paste0(" - ", body_txt) else "")
    stop(msg, call. = FALSE)
  }
  invisible(TRUE)
}

# Utility: build endpoint URL ----------------------------------------------

# type: "grid", "timeseries", "station"
# mode: "historical", "current", "forecast"
# resource_id: e.g. "tawes-v1-10min", "synop-v1-1h", "spartacus-v2-1d-1km"
build_dataset_url <- function(type, mode, resource_id) {
  paste0(
    GEOSPHERE_BASE_URL, "/",
    match.arg(type, c("grid", "timeseries", "station")),
    "/",
    match.arg(mode, c("historical", "current", "forecast")),
    "/",
    resource_id
  )
}

# Added: shared HTTP GET with retry + timeout
geosphere_get <- function(url, query = NULL) {
  httr::RETRY(
    "GET", url, query = query,
    times = GEOSPHERE_RETRY_TIMES,
    pause_min = GEOSPHERE_RETRY_PAUSE_MIN,
    pause_cap = GEOSPHERE_RETRY_PAUSE_CAP,
    terminate_on = GEOSPHERE_TERMINATE_ON,
    httr::timeout(GEOSPHERE_TIMEOUT_SECONDS)
  )
}

# List all available datasets ----------------------------------------------

# Returns a data.frame with:
#   key, type, mode, response_formats, url
list_datasets <- function() {
  resp <- geosphere_get(paste0(GEOSPHERE_BASE_URL, "/datasets"))
  stop_for_status_with_body(resp)
  
  txt <- httr::content(resp, as = "text", encoding = "UTF-8")
  js  <- jsonlite::fromJSON(txt)
  
  # js is a named list; each element is a list with type/mode/response_formats/url
  keys  <- names(js)
  types <- vapply(js, function(x) x$type, character(1))
  modes <- vapply(js, function(x) x$mode, character(1))
  fmts  <- vapply(js, function(x) paste(x$response_formats, collapse = "|"), character(1))
  urls  <- vapply(js, function(x) x$url, character(1))
  
  data.frame(
    key              = keys,
    type             = types,
    mode             = modes,
    response_formats = fmts,
    url              = urls,
    stringsAsFactors = FALSE
  )
}

# Get metadata for a dataset -----------------------------------------------

# section:
#   - "json"       -> /metadata  (default)
#   - "stations"   -> /metadata/stations  (CSV; station-type datasets, mode = historical)
#   - "parameters" -> /metadata/parameters (CSV; station-type datasets, mode = historical)
# output_format:
#   - for "json" section: always returns list
#   - for CSV sections: returns data.frame
get_metadata <- function(type,
                         mode,
                         resource_id,
                         section = c("json", "stations", "parameters")) {
  
  section <- match.arg(section)
  
  base_url <- build_dataset_url(type, mode, resource_id)
  
  if (section == "json") {
    url <- paste0(base_url, "/metadata")
    resp <- geosphere_get(url)
    stop_for_status_with_body(resp)
    
    txt <- httr::content(resp, as = "text", encoding = "UTF-8")
    jsonlite::fromJSON(txt, simplifyVector = TRUE)
    
  } else if (section %in% c("stations", "parameters")) {
    # These CSV metadata endpoints are currently documented for
    # station / historical datasets (see API: Grundlagen, Beispiel 2).
    url <- paste0(base_url, "/metadata/", section)
    resp <- geosphere_get(url)
    stop_for_status_with_body(resp)
    
    txt <- httr::content(resp, as = "text", encoding = "UTF-8")
    readr::read_csv(txt, show_col_types = FALSE)
  }
}

# Generic download function ------------------------------------------------

# type: "grid", "timeseries", "station"
# mode: "historical", "current", "forecast"
# resource_id: e.g. "tawes-v1-10min"
#
# parameters: vector of parameter names (e.g. c("TL", "RR", "FF"))
# station_ids: numeric or character vector of station IDs (station type) – will be
#              turned into "11035,11216" etc.
# bbox: numeric vector c(south, west, north, east) for grid type
# lat_lon: for timeseries type; either numeric c(lat, lon) or a list of such vectors
# start, end: character strings "YYYY-MM-DD" or "YYYY-MM-DDThh:mm"
# forecast_offset: integer (for forecast mode)
#
# output_format: "csv", "geojson", "netcdf", "json"
#   - csv  -> returns data.frame (if out_file = NULL) or writes file
#   - json -> returns parsed JSON (list)
#   - geojson/netcdf -> usually you will want to write to file (out_file);
#                       otherwise raw content is returned
#
# extra_params: named list of additional query parameters, if needed
#
# out_file: optional file path to write the response to.
#           - if NULL and output_format == "csv": returns data.frame
#           - if NULL and output_format == "json": returns parsed JSON
#           - if NULL and other formats: returns raw bytes
download_dataset <- function(type,
                             mode,
                             resource_id,
                             parameters      = NULL,
                             station_ids     = NULL,
                             bbox            = NULL,
                             lat_lon         = NULL,
                             start           = NULL,
                             end             = NULL,
                             forecast_offset = NULL,
                             output_format   = c("csv", "geojson", "netcdf", "json"),
                             extra_params    = list(),
                             out_file        = NULL,
                             verbose         = TRUE) {
  
  output_format <- match.arg(output_format)
  
  url <- build_dataset_url(type, mode, resource_id)
  
  # Build query list according to docs (parameters, station_ids, bbox, lat_lon,
  # start, end, forecast_offset, output_format, ...)
  query <- list()
  
  # parameters (array, comma-separated string allowed)
  if (!is.null(parameters)) {
    query$parameters <- paste(parameters, collapse = ",")
  }
  
  # station_ids (array, comma-separated)
  if (!is.null(station_ids)) {
    query$station_ids <- paste(station_ids, collapse = ",")
  }
  
  # bbox for grid type: south,west,north,east
  if (!is.null(bbox)) {
    if (length(bbox) != 4) {
      stop("bbox must be a numeric vector of length 4: c(south, west, north, east).")
    }
    query$bbox <- paste(bbox, collapse = ",")
  }
  
  # lat_lon for timeseries type: can be repeated (lat_lon=48,12&lat_lon=48.5,11.3 ...)
  if (!is.null(lat_lon)) {
    if (is.numeric(lat_lon) && length(lat_lon) == 2) {
      query$lat_lon <- paste(lat_lon, collapse = ",")
    } else if (is.list(lat_lon)) {
      query$lat_lon <- vapply(lat_lon, function(x) paste(x, collapse = ","), character(1))
    } else {
      stop("lat_lon must be numeric length 2 or a list of numeric length-2 vectors.")
    }
  }
  
  # mode-specific parameters: start, end, forecast_offset
  if (!is.null(start)) {
    query$start <- start
  }
  if (!is.null(end)) {
    query$end <- end
  }
  if (!is.null(forecast_offset)) {
    query$forecast_offset <- as.integer(forecast_offset)
  }
  
  # Output format for the data itself (csv, geojson, ...)
  query$output_format <- output_format
  
  # Merge in any additional query params
  if (length(extra_params) > 0) {
    for (nm in names(extra_params)) {
      query[[nm]] <- extra_params[[nm]]
    }
  }
  
  if (verbose) {
    message("Requesting: ", url)
    message("Query: ", paste(sprintf("%s=%s", names(query), query), collapse = "&"))
  }
  
  resp <- geosphere_get(url, query = query)
  stop_for_status_with_body(resp)
  
  # If a file path is given, write raw content to that file
  raw_content <- httr::content(resp, as = "raw")
  
  if (!is.null(out_file)) {
    dir.create(dirname(out_file), showWarnings = FALSE, recursive = TRUE)
    writeBin(raw_content, out_file)
    if (verbose) {
      message("Saved response to: ", out_file)
    }
    return(invisible(out_file))
  }
  
  # No out_file: parse depending on output_format
  if (output_format == "csv") {
    txt <- httr::content(resp, as = "text", encoding = "UTF-8")
    return(readr::read_csv(txt, show_col_types = FALSE))
    
  } else if (output_format == "json") {
    txt <- httr::content(resp, as = "text", encoding = "UTF-8")
    return(jsonlite::fromJSON(txt, simplifyVector = TRUE))
    
  } else {
    # geojson or netcdf – return raw bytes; user can handle as needed
    return(raw_content)
  }
}

# Convenience wrapper for station data -------------------------------------

# Example wrapper specifically for station-type datasets.
download_station_data <- function(mode,
                                  resource_id,
                                  parameters      = NULL,
                                  station_ids     = NULL,
                                  start           = NULL,
                                  end             = NULL,
                                  output_format   = c("csv", "geojson", "json"),
                                  extra_params    = list(),
                                  out_file        = NULL,
                                  verbose         = TRUE) {
  
  output_format <- match.arg(output_format)
  
  download_dataset(
    type            = "station",
    mode            = mode,
    resource_id     = resource_id,
    parameters      = parameters,
    station_ids     = station_ids,
    start           = start,
    end             = end,
    output_format   = output_format,
    extra_params    = extra_params,
    out_file        = out_file,
    verbose         = verbose
  )
}

######################################################################
# EXAMPLES (uncomment to run)
######################################################################

# # 1) List all available datasets
# ds <- list_datasets()
# 
# # We found what we are interested in:
# #/station/historical/klima-v2-1h
# 
# # 2) Get JSON metadata for current TAWES 10min station dataset
# meta_climate_1h <- get_metadata("station", "historical", "klima-v2-1h")
# str(meta_climate_1h, max.level = 1)
# 
# # 3) Get station & parameter metadata in CSV for historical TAWES
# #    (only valid for station/historical datasets)
# station_climate_1h   <- get_metadata("station", "historical", "klima-v2-1h", "stations")
# parameters_climate_1h <- get_metadata("station", "historical", "klima-v2-1h", "parameters")
# 
# # 4) Examples to download data in csv format
# df_station_climate_1h <- download_station_data(
#   mode          = "historical",
#   resource_id   = "klima-v2-1h",
#   parameters    = c("TL","RR","CGLO","RF","FF","SH"),  # temperature, precipitation, global radiation, humidity, wind speed, snow depth
#   station_ids   = 11803,                # Innsbruck
#   start         = "2023-01-01",
#   end           = "2023-12-31",
#   output_format = "csv"
# )
# 
# df_station_tawes_10min <- download_station_data(
#   mode          = "historical",
#   resource_id   = "tawes-v1-10min",
#   parameters    = c("TL", "RR", "FF"),  # temp, precipitation, wind speed
#   station_ids   = 11035,                # Wien Hohe Warte
#   start         = "2025-11-20",
#   end           = "2025-11-21",
#   output_format = "csv"
# )