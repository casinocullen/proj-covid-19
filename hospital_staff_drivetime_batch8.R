# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Number of staff  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))


library(tidyverse)
library(osrm)
library(sf)
library(lwgeom)
# Update: 4/4
# Est. drive-time

x.npi.filter.batch.8= readRDS("./output/x.npi.filter.batch.8.RDS") %>% 
  dplyr::filter(round(lon) != 0 & round(lat) != 0)

# x.npi.filter.done = st_read('./output/x.npi.filter.dt.batch.3.gpkg') 
# 
# x.npi.filter.dt.batch.3 = x.npi.filter.done %>% 
#   dplyr::rename(geometry = geom)
i = 1

x.npi.filter.dt.batch.8 = NULL
while (i <= nrow(x.npi.filter.batch.8)) {

  # if(x.npi.filter.batch.8[i, 'npi'] %in% x.npi.filter.dt.batch.5$npi){
  #   i = i + 1
  #   print(paste0("====Skipped: ", x.npi.filter.batch.5.unfinished[i, 'npi'], " already done!====="))
  #   next
  # }

  onepoint_info = x.npi.filter.batch.8[i, ]
  onepoint_info_expanded <- onepoint_info[rep(row.names(onepoint_info), 1), ]
  
  onepoint = NULL
  attempt = 1
  while(is.null(onepoint)) {
    tryCatch(expr = {
      onepoint <- osrmIsochrone(loc = c(x.npi.filter.batch.8[i,"lon"], x.npi.filter.batch.8[i,"lat"]), breaks = c(75),
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
  
  if(nrow(onepoint) == 1){
    print(paste0("SUCCESSFULLY INPUT ", x.npi.filter.batch.8[i,"npi"]))
    onepoint_info_expanded = bind_cols(onepoint, onepoint_info_expanded)
    x.npi.filter.dt.batch.8 = rbind(x.npi.filter.dt.batch.8, onepoint_info_expanded)
    i = i + 1
  }
  
  if(i %% 500 == 0){
    print(paste0("====Finished: ", as.character(i), " points!====="))
    sf::st_write(x.npi.filter.dt.batch.8,  "./output/x.npi.filter.dt.batch.8.gpkg", delete_dsn = T)
  }
  
}

