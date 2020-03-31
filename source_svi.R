# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# CDC SVI  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))


source("../base/global.R")

x.src.county = st_read('./source/SVI2018_US_county.shp') %>% 
  st_drop_geometry()

dbWriteTable(coririsi_source, "cdc_svi", x.src.county)

