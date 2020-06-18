# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Number of staff  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))

source("../base/global.R")
library(tidyverse)
library(osrm)
library(sf)
library(lwgeom)
# Update: 4/4
# Est. drive-time

x.npi.filter.batch.8= readRDS("./output/x.npi.filter.batch.8.RDS") %>% 
  dplyr::filter(round(lon) != 0 & round(lat) != 0)

x.definitive.health = st_read(coririsi_layer, "definitive_healthcare_hospital_beds") %>% 
  janitor::clean_names() %>% 
  dplyr::filter(round(longitude) != 0 & round(latitude) != 0) %>% 
  st_drop_geometry()

i = 1
x.definitive.health.dt = NULL

while (i <= nrow(x.definitive.health)) {

  # if(x.npi.filter.batch.8[i, 'npi'] %in% x.npi.filter.dt.batch.5$npi){
  #   i = i + 1
  #   print(paste0("====Skipped: ", x.npi.filter.batch.5.unfinished[i, 'npi'], " already done!====="))
  #   next
  # }

  onepoint_info = x.definitive.health[i, ]
  onepoint_info_expanded <- onepoint_info[rep(row.names(onepoint_info), 2), ]
  
  onepoint = NULL
  attempt = 1
  while(is.null(onepoint)) {
    tryCatch(expr = {
      onepoint <- osrmIsochrone(loc = c(x.definitive.health[i,"longitude"], x.definitive.health[i,"latitude"]), breaks = c(40, 75),
                                returnclass="sf") %>% 
        dplyr::select(driveTime = max, geometry)}, 
      error=function(e)
      {
        print(paste0("ERROR: ", e, ", ATTEMPT: ", attempt))
      }
    )
    
    attempt = attempt + 1
    if (attempt >= 10) {
      i = i + 1
      next
    }
  }
  
  if(nrow(onepoint) == 2){
    print(paste0("SUCCESSFULLY INPUT ", x.definitive.health[i,"objectid"]))
    onepoint_info_expanded = bind_cols(onepoint, onepoint_info_expanded)
    x.definitive.health.dt = rbind(x.definitive.health.dt, onepoint_info_expanded)
    i = i + 1
  }
  
  if(i %% 500 == 0){
    print(paste0("====Finished: ", as.character(i), " points!====="))
    write_layer(x.definitive.health.dt, "definitive_healthcare_hospital_drivetime", db = T)
  }
  
}

# Write layer
write_layer(x.definitive.health.new.dt.all, "definitive_healthcare_hospital_drivetime", db = T, new.server = T, new.server.overwrite = T)


