###############################################################
#                                                             #
# SCRIPT TO DOWNLOAD STATION DATA FROM ItaliaMeteo help funcs #
#                                                             #
###############################################################
library(httr2)
library(purrr)
library(data.table)

login = function(username, password) {
  
  req <- request(
    "https://meteohub.agenziaitaliameteo.it/auth/login"
  ) |>
    req_method("POST") |>
    req_body_json(list(
      username = username,
      password = password
    )) |>
    req_perform()
  
  # resp_body_json() auf das Response-Objekt aufrufen
  login_token <- resp_body_json(req, simplifyVector = TRUE)
  return(login_token)
}

get_datasets = function(bb_wgs) {
  
  # get all available datasets
  datasets <- request(
    "https://meteohub.agenziaitaliameteo.it/api/datasets"
  ) |>
    req_headers(accept = "application/json") |>
    req_perform() |>
    resp_body_json()
  
  
  dt_list <- map(datasets, as.data.table)
  dt <- rbindlist(dt_list, fill = TRUE, idcol = T)
  
  
  # filter datasets so only observed data remains
  obs_datasets <- dt[category == "OBS"]
  
  # select only datasets, that are part of the dpcn network
  dpcn_datasets <- obs_datasets[grepl("^dpcn", id)]
  
  # 5. Bounding Polygons in sf
  dpcn_sf <- st_as_sf(dpcn_datasets, wkt = "bounding", crs = 4326)
  
  # 6. bbox-Filter
  inside_bbox <- st_intersects(dpcn_sf, bb_wgs, sparse = FALSE)[,1]
  dpcn_sf <- dpcn_sf[inside_bbox, ]
  
  return(dpcn_sf)
}



download_dpcn_task_json <- function(network_id, start_date, end_date, login_token) {
  
  # 1. Download-Task starten
  task_req <- request("https://meteohub.agenziaitaliameteo.it/api/data") |>
    req_method("POST") |>
    req_body_json(list(
      request_name = username,
      dataset_names = list(network_id),
      reftime = list(
        from = paste0(start_date, "T00:00:00Z"),
        to   = paste0(end_date, "T00:00:00Z")
      ),
      output_format = "json"
    )) |>
    req_headers(
      Authorization = paste("Bearer", login_token)
    )
  
  task_resp <- req_perform(task_req)
  task_json <- resp_body_json(task_resp, simplifyVector = TRUE)
  task_id <- task_json$task_id
  cat("Download task gestartet:", task_id, "\n")
  
  # 2. Prüfen, bis Task fertig ist
  repeat {
    status_req <- request(paste0("https://meteohub.agenziaitaliameteo.it/api/task/", task_id)) |>
      req_headers(Authorization = login_token)
    
    status_resp <- req_perform(status_req)
    status_json <- resp_body_json(status_resp, simplifyVector = TRUE)
    
    if (status_json$status == "SUCCESS") {
      cat("Download fertig! Datei:", status_json$fileoutput, "\n")
      break
    } else {
      cat("Noch nicht fertig, Status:", status_json$status, "\n")
      Sys.sleep(5)
    }
  }
  
  # 3. JSON-Daten herunterladen
  data_req <- request(paste0("https://meteohub.agenziaitaliameteo.it/api/files/", status_json$fileoutput)) |>
    req_headers(Authorization = login_token)
  
  data_resp <- req_perform(data_req)
  data_json <- resp_body_json(data_resp, simplifyVector = TRUE)
  
  return(data_json)
}




