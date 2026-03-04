###################################################################
#                                                                 #
# SCRIPT TO DOWNLOAD Station Data FROM Deutscher Wetterdienst     #
#                                                                 #
###################################################################

# Required packages:
# sf, readr, dplyr, lubridate, zip, purrr, httr2, xml2, stringr

# Source functions
source("scripts/getStationData_DE/DWDAccessFunctions.R")

getStationData_DE <- function(bbox, start_date, end_date) {
  
  out_dir <- "output/dwd_station_data"
  dir.create(out_dir, showWarnings = FALSE)
  
  start_date <- as.POSIXct(start_date, tz = "UTC")
  end_date   <- as.POSIXct(end_date,   tz = "UTC")
  
  stationen_sf_3416 = getDWDStations()
  
  stations_in_bbox <- stationen_sf_3416 %>%
    filter(st_within(geometry, bbox, sparse = FALSE))
  
  # 2. Abbruch, wenn keine Stationen
  if (nrow(stations_in_bbox) == 0) {
    message("→ Keine DWD-Stationen in der Bounding Box")
    return(invisible(NULL))
  }
  
  message("→ ", nrow(stations_in_bbox), " Stationen gefunden")
  
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
  
  # Funktion für Datumskonvertierung
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
        cat("  → keine Daten\n")
        next
      }
      
      zip_url  <- paste0(cfg$base_url, zip_name)
      zip_file <- file.path(out_dir, paste0(var, "_", sid, ".zip"))
      extract_dir <- file.path(out_dir, paste0("station_", sid))
      dir.create(extract_dir, showWarnings = FALSE)
      
      # Download
      resp <- request(zip_url) |> req_timeout(60) |> req_perform()
      if (resp_status(resp) != 200) {
        cat("  → Download fehlgeschlagen\n")
        next
      }
      
      writeBin(resp_body_raw(resp), zip_file)
      unzip(zip_file, exdir = extract_dir)
      
      # Produktdatei finden
      txt_file <- list.files(
        extract_dir,
        pattern = cfg$product_pattern,
        full.names = TRUE
      )
      
      if (length(txt_file) != 1) {
        cat("  → Produktdatei fehlt\n")
        next
      }
      
      df <- read_delim(
        txt_file,
        delim = ";",
        col_types = cols(),
        locale = locale(decimal_mark = "."),
        trim_ws = TRUE
      )
      
      # --- Fehlwerte bereinigen (-999 zu NA) ---
      df <- df |> 
        mutate(across(where(is.numeric), ~na_if(., -999)))
      
      # --- Datumskonvertierung und Upsampling ---
      if (var == "SH") {
        # 1. Datum als reines Tagesdatum einlesen
        df <- df |>
          mutate(
            MESS_DATUM_DAY = as.Date(as.character(MESS_DATUM), format = "%Y%m%d")
          ) |>
          filter(
            MESS_DATUM_DAY >= as.Date(start_date),
            MESS_DATUM_DAY <= as.Date(end_date)
          )
        
        if (nrow(df) == 0) {
          cat("  → keine Daten im Zeitraum\n")
          next
        }
        
        # 2. Upsampling: Jede Zeile (Tag) in 24 Stunden expandieren
        df <- df |>
          group_by(STATIONS_ID, MESS_DATUM_DAY) |>
          reframe(
            # Wir bauen die Stunden nur für den aktuell gruppierten Tag
            MESS_DATUM = seq(
              from = as.POSIXct(paste(MESS_DATUM_DAY, "00:00:00"), tz = "UTC"),
              by   = "hour",
              length.out = 24
            ),
            # Alle anderen Spalten (wie SHK_TAG) einfach wiederholen
            across(where(is.numeric), first)
          ) |>
          ungroup() |>
          select(-MESS_DATUM_DAY)
        
      } else {
        # Normaler stündlicher Ablauf für TU, RR, FF, ST
        df <- df |>
          mutate(
            MESS_DATUM = convert_mess_datum(MESS_DATUM)
          ) |>
          filter(
            MESS_DATUM >= start_date,
            MESS_DATUM <= end_date
          )
      }
      
      if (nrow(df) == 0) {
        cat("  → keine Daten im Zeitraum\n")
        next
      }
      
      # Nur die Station id, das Messdatum und die Variabel wird ausgewählt
      df_out <- df |> select(any_of(cfg$select_cols))
      
      # Pfad für die output csv wird erstellt
      out_csv <- file.path(
        extract_dir,
        paste0("station_", sid, "_", var, ".csv")
      )
      
      # csv für station und variabel wird erstellt
      write_csv(df_out, out_csv)
      cat("  → OK\n")
    }
  }
  
  # Funktion um für jede Station die Variablen in eine csv zu mergen
  merge_station_vars <- function(station_id, base_dir = "dwd_station_data") {
    
    sid <- sprintf("%05d", as.integer(station_id))
    station_dir <- file.path(base_dir, paste0("station_", sid))
    
    # --- TU als Basis ---
    tu_file <- file.path(station_dir, paste0("station_", sid, "_TU.csv"))
    if (!file.exists(tu_file)) return(NULL)
    
    df_tu <- read_csv(tu_file, show_col_types = FALSE) |>
      distinct(STATIONS_ID, MESS_DATUM, .keep_all = TRUE)
    
    df_all <- df_tu
    
    # --- optionale Variablen ---
    other_vars <- c("RR", "FF", "ST", "SH")
    
    for (v in other_vars) {
      
      f <- file.path(station_dir, paste0("station_", sid, "_", v, ".csv"))
      if (!file.exists(f)) next
      
      df_v <- read_csv(f, show_col_types = FALSE) |>
        distinct(STATIONS_ID, MESS_DATUM, .keep_all = TRUE)
      
      df_all <- left_join(
        df_all,
        df_v,
        by = c("STATIONS_ID", "MESS_DATUM")
      )
    }
    
    df_all
  }
  
  
  
  
  for (sid in stations_in_bbox$Stations_id) {
    
    df <- merge_station_vars(sid)
    if (is.null(df)) next
    
    station_name <- stations_in_bbox |>
      filter(Stations_id == sid) |>
      pull(Stationsname)
    
    out_file <- file.path(
      "dwd_station_data",
      paste0("station_", 
             sprintf("%05d", as.integer(sid)),
             "_", 
             station_name, 
             "_merged.csv")
    )
    
    write_csv(df, out_file)
  }
  return(stations_in_bbox)
}




