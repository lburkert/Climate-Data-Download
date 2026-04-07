###############################################################
#                                                             #
#    SCRIPT TO DOWNLOAD STATION METADATA FROM METEO SWISS     #
#                                                             #
###############################################################

# make station names safe for filenames (no spaces, slashes, umlauts etc.)
make_safe_name <- function(x) {
  x <- iconv(x, to = "ASCII//TRANSLIT")        # transliterate umlauts etc.
  x <- gsub("[^A-Za-z0-9]+", "-", x)          # replace any non-alphanumeric with "-"
  x <- gsub("-+", "-", x)                     # collapse multiple "-"
  x <- gsub("^-|-$", "", x)                   # trim leading/trailing "-"
  if (!nzchar(x)) x <- "NA"
  x
}

getMeteoSwissStations = function(bbox){
  
  # transform bbox from 3416 to 4236 for the server
  poly_4326 <- st_transform(bbox, 4326)
  bbox_4326 <- st_bbox(poly_4326)
  
  # create url from bbox to fetch station metadata
  stac_url <- paste0(
    "https://data.geo.admin.ch/api/stac/v0.9/collections/ch.meteoschweiz.ogd-smn/items?",
    "bbox=", 
    as.numeric(bbox_4326["xmin"]), ",", as.numeric(bbox_4326["ymin"]), ",", as.numeric(bbox_4326["xmax"]), ",", as.numeric(bbox_4326["ymax"]),
    "&limit=1000"
  )
  
  # get metadata from url
  res <- request(stac_url) |>
    req_perform() |>
    resp_check_status()
  
  # extract metadata
  items <- res |>
    resp_body_string(encoding = "Windows-1252") |>
    fromJSON(simplifyVector = FALSE) |>
    (\(x) x$features)()
  
  # create empty dataframe
  df <- data.frame(
    id    = character(),
    lon   = numeric(),
    lat   = numeric(),
    title = character(),
    stringsAsFactors = FALSE
  )
  
  # create list for .csv filepaths
  df$assets <- I(list())
  
  rows <- vector("list", length(items))
  
  i <- 1
  for (item in items) {
    
    asset_names <- names(item$assets)
    asset_names <- asset_names[grepl("_h_.*historical.*\\.csv$", asset_names)]
    
    asset_hrefs <- sapply(asset_names, function(n)
      item$assets[[n]]$href
    )
    
    coords <- unlist(item$geometry$coordinates)
    
    rows[[i]] <- list(
      id = item$id,
      lon = coords[1],
      lat = coords[2],
      title = make_safe_name(item$properties$title),
      assets = list(asset_hrefs)
    )
    
    i <- i + 1
  }
  
  df <- bind_rows(rows)
  
  # create sf object and transform to 3416 while keeping lat and lon for optional netcdf output 
  df_sf <- st_as_sf(df, coords = c("lon", "lat"), crs = 4326)
  df_sf <- st_transform(df_sf, 3416)
  
  return(df_sf)
}