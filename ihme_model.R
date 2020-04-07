# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# IHME  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))


source("../base/global.R")
x.date <- as.character(Sys.Date(), format = '%m_%d_%Y')



# Source Table
url_to_s3(url = "https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip", 
          filename = "ihme-covid19.zip", 
          s3path = "source/proj-covid-19/", 
          s3bucket =  "cori-layers")

unzip("./source/ihme-covid19.zip", exdir = "./source/ihme/", junkpaths = T) 


x.src.ihme = fread("./source/ihme/Hospitalization_all_locs.csv")

#dbWriteTable(coririsi_source, "ihme_state_projection", x.src.ihme, overwrite = T)


# Layer table
x.geo.state = st_read(coririsi_layer, 'geo_attr_state_pg')

x.layer.ihme = x.src.ihme %>% 
  dplyr::mutate(date = as.Date(date, format = "%Y-%m-%d")) %>% 
  group_by(location_name) %>% 
  dplyr::mutate(ICU_bed_max_date = case_when(max(ICUbed_mean) == ICUbed_mean ~ date),
                ICU_bed_max_needed = case_when(max(ICUbed_mean) == ICUbed_mean ~ ICUbed_mean),
                allbed_max_date = case_when(max(allbed_mean) == allbed_mean ~ date),
                allbed_max_needed = case_when(max(allbed_mean) == allbed_mean ~ allbed_mean),
                InvVen_max_date = case_when(max(InvVen_mean) == InvVen_mean ~ date),
                InvVen_max_needed = case_when(max(InvVen_mean) == InvVen_mean ~ InvVen_mean),
                newICU_max_date = case_when(max(newICU_mean) == newICU_mean ~ date),
                newICU_max_needed = case_when(max(newICU_mean) == newICU_mean ~ newICU_mean),
                admis_max_date = case_when(max(admis_mean) == admis_mean ~ date),
                admis_max_needed = case_when(max(admis_mean) == admis_mean ~ admis_mean),
                icuover_max_date = case_when(max(icuover_mean) == icuover_mean ~ date),
                icuover_max_needed = case_when(max(icuover_mean) == icuover_mean ~ icuover_mean)) %>% 
  ungroup() %>% 
  group_by(location_name) %>% 
  dplyr::summarise_all(funs(max(., na.rm = TRUE))) %>%
  dplyr::select(location_name, ends_with("_date"), ends_with("_needed")) %>% 
  janitor::clean_names() %>%
  dplyr::mutate(last_update = x.date)

x.layer.ihme.geo = x.geo.state %>%
  left_join(x.layer.ihme, by = c('name' = 'location_name')) %>%
  drop_na(icuover_max_needed)

write_layer(x.layer.ihme.geo, paste0("ihme_peak_dates_",x.date), db =T , fs = T, ngacarto = T, ngacarto.overwrite = T, new.server.overwrite = T, new.server = T)

