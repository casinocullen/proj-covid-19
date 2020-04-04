# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Number of staff  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))


source("../base/global.R")


# Update: 4/4
# Est. drive-time

x.npi.filter.batch.2 = readRDS("./output/x.npi.filter.batch.2.RDS")

x.npi.filter.dt.batch.2 = NULL
i = 1
while (i <= nrow(x.npi.filter.batch.2)) {
  onepoint_info = x.npi.filter.batch.2[i, ]
  onepoint_info_expanded <- onepoint_info[rep(row.names(onepoint_info), 2), ]
  
  onepoint = NULL
  while(is.null(onepoint)) {
    try(
      onepoint <- osrmIsochrone(loc = c(x.npi.filter.batch.2[i,"lon"], x.npi.filter.batch.2[i,"lat"]), breaks = c(40,75),
                                returnclass="sf") %>% 
        dplyr::select(driveTime = max, geometry))
    
  }
  if(nrow(onepoint) == 2){
    print(paste0("SUCCESSFULLY INPUT ", x.npi.filter.batch.2[i,"npi"]))
    onepoint_info_expanded = bind_cols(onepoint_info_expanded, onepoint)
    x.npi.filter.dt.batch.2 = rbind(x.npi.filter.dt.batch.2, onepoint_info_expanded)
    i = i + 1
  }
}



