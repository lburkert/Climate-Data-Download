library(httr2)
library(jsonlite)
library(sf)
library(dplyr)
library(this.path)

# Set working directory auf den Script-Ordner
script_path = this.path()
setwd(dirname(script_path))

# Source functions
source("MeteoItalia_access_functions.R")

bbox_3416 <- c(
  xmin = 229700,  # westliche Grenze
  xmax = 289500,  # östliche Grenze
  ymin = 308000,  # südliche Grenze
  ymax = 362000   # nördliche Grenze
)

bbox_poly_3416 <- st_as_sfc(st_bbox(bbox_3416, crs = 3416))

username = "lukas.burkert@gmx.de"
password = "Meteo!2026"


outdir = "output/meteo_italia/"
dir.create(out_dir, showWarnings = FALSE)

# reproject to WGS84 for ItaliaMeteo
bb_wgs <- st_transform(bbox_poly_3416, 4326)

# ---------------------------
# LOGIN
# ---------------------------
login_token = login(username, password)

# get the region datasets, that are in our bounding box
datasets = get_datasets(bb_wgs)

# get the id for the download request
dataset_id = datasets$name

# download data
data = download_dpcn_task_json(dataset_id, start_date, end_date, login_token)
  



# ---------------------------
# WAIT UNTIL READY
# ---------------------------
message("3) Waiting for processing...")

repeat {
  
  Sys.sleep(10)
  
  status <- request(
    "https://meteohub.agenziaitaliameteo.it/api/requests"
  ) |>
    req_headers(
      Authorization = paste("Bearer", token)
    ) |>
    req_perform()
  
  requests <- resp_body_json(status)
  
  latest <- requests |>
    dplyr::slice_tail(n = 1)
  
  if(!is.null(latest$fileoutput)){
    filename <- latest$fileoutput
    break
  }
  
  message("   still processing...")
}

# ---------------------------
# DOWNLOAD
# ---------------------------
message("4) Downloading data...")

download <- request(
  paste0(
    "https://meteohub.agenziaitaliameteo.it/api/data/",
    filename
  )
) |>
  req_headers(
    Authorization = paste("Bearer", token)
  ) |>
  req_perform()

outfile <- file.path(outdir, filename)

writeBin(resp_body_raw(download), outfile)

message("Saved: ", outfile)


# ---------------------------
# READ DATA → SF
# ---------------------------
message("5) Convert to sf...")

dat <- jsonlite::fromJSON(outfile, flatten = TRUE)

stations_sf <- st_as_sf(
  dat,
  coords = c("longitude","latitude"),
  crs = 4326
)

return(stations_sf)
}
