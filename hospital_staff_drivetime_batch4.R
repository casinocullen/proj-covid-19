# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Number of staff  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))


library(tidyverse)
library(osrm)
library(sf)
library(lwgeom)

x.npi.filter.batch.4= readRDS("./output/x.npi.filter.batch.4.RDS") %>% 
  dplyr::filter(round(lon) != 0 & round(lat) != 0)

x.npi.filter.done = st_read('./output/x.npi.filter.dt.batch.4.gpkg') 

x.npi.filter.dt.batch.4 = x.npi.filter.done %>% 
  dplyr::rename(geometry = geom)

# Update: 4/4
# Est. drive-time

i = 1
while (i <= nrow(x.npi.filter.batch.4.unfinished.b2)) {
  
  if(x.npi.filter.batch.4.unfinished.b2[i, 'npi'] %in% x.npi.filter.dt.batch.4$npi){
    i = i + 1
    print(paste0("====Skipped: ", x.npi.filter.batch.4.unfinished.b2[i, 'npi'], " already done!====="))
    next
  }
  
  onepoint_info = x.npi.filter.batch.4.unfinished.b2[i, ]
  onepoint_info_expanded <- onepoint_info[rep(row.names(onepoint_info), 2), ]
  
  onepoint = NULL
  attempt = 1
  while(is.null(onepoint)) {
    tryCatch(expr = {
      onepoint <- osrmIsochrone(loc = c(x.npi.filter.batch.4.unfinished.b2[i,"lon"], x.npi.filter.batch.4.unfinished.b2[i,"lat"]), breaks = c(40,75),
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
    print(paste0("SUCCESSFULLY INPUT ", x.npi.filter.batch.4.unfinished.b2[i,"npi"]))
    onepoint_info_expanded = bind_cols(onepoint, onepoint_info_expanded)
    x.npi.filter.dt.batch.4 = rbind(x.npi.filter.dt.batch.4, onepoint_info_expanded)
    i = i + 1
  }
  
  if(i %% 500 == 0){
    print(paste0("====Finished: ", as.character(i), " points!====="))
    x.npi.filter.dt.batch.4 = x.npi.filter.dt.batch.4 %>%
      st_as_sf()
    st_write(x.npi.filter.dt.batch.4, "./output/x.npi.filter.dt.batch.4.gpkg")
  }
  
  
}




`%notin%` <- Negate(`%in%`)

x.npi.filter.batch.4.unfinished = x.npi.filter.batch.4 %>% 
  dplyr::filter(npi %notin% x.npi.filter.dt.batch.4$npi)

x.npi.filter.batch.4.unfinished.b1 = x.npi.filter.batch.4.unfinished[c(0:7000),]
x.npi.filter.batch.4.unfinished.b2 = x.npi.filter.batch.4.unfinished[c(7001:14373),]


saveRDS(x.npi.filter.batch.4.unfinished.b1, "x.npi.filter.batch.4.unfinished.RDS")


st_write(x.npi.filter.dt.batch.2, "./output/x.npi.filter.dt.batch.2.gpkg")
write_layer(x.npi.filter.dt.batch.2, "npi_drivetime_40_75_b2", db = T)

