# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# SETUP  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====

require(tidyverse)
require(sf)
require(DBI)
require(data.table)
require(scales)
source("/data/Github/base/functions/write_layer_absolute.R")
source("/data/Github/base/functions/searchDB.R")
`%notin%` <- Negate(`%in%`)

states.non.terr <- c("AS", "PR", "GU", "MP", "VI", "UM")

x.ihme.results = searchDB(sterm.with = "^ihme_peak_dates_", x.conn = coririsi_layer)

x.ihme.alls = dbGetQuery(coririsi_layer, "SELECT name, geoid, stusps FROM geo_attr_state_pg") %>%
  dplyr::filter(stusps %notin% states.non.terr) %>%
  dplyr::arrange(name)



#Cbind all dates from IHME
for (table_name in x.ihme.results) {
  x.ihme.one.date = as.Date(str_extract(table_name, "\\d+_\\d+_\\d+"), format = "%m_%d_%Y")
  x.ihme.one = st_read(coririsi_layer, table_name) %>% 
    st_drop_geometry() %>% 
    dplyr::select(name, geoid, stusps, ends_with("_date"), ends_with("_needed")) %>%
    dplyr::rename_at(vars(ends_with("_date")|ends_with("_needed")),  .funs = funs(paste0(., "_",x.ihme.one.date))) %>%
    dplyr::arrange(name)
  
  x.ihme.alls = x.ihme.alls %>% 
    left_join(x.ihme.one, by = c("name", "geoid", "stusps")) %>% 
    dplyr::filter()
}

x.ihme.alls.icu.short = x.ihme.alls %>% 
  dplyr::select(name, geoid, stusps, starts_with("icuover_"))

#Write to GS

ss = "https://docs.google.com/spreadsheets/d/1aaJGicis89sm2_dPCkF8kBL1RwDCTEQM4rvKKhsvts4/edit#gid=0"
googlesheets4::sheets_write(ss, data = x.ihme.alls, sheet = "Import From R")
googlesheets4::sheets_write(ss, data = x.ihme.alls.icu.short, sheet = "ICU Shortage")
