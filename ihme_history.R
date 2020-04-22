# Source Table
url = "https://ihmecovid19storage.blob.core.windows.net/archive/2020-03-30/ihme-covid19.zip"
download.file(url, destfile = "./source/ihme-covid19.zip")


utils::unzip("./source/ihme-covid19.zip", exdir = "./source/ihme/", junkpaths = T, overwrite = T) 
utils::unzip("./source/ihme-covid19.zip", exdir = "./source/ihme/", overwrite = T)

# Find the latest ihme update
all_ihme_dirs = list.dirs(path = "./source/ihme/", full.names = TRUE, recursive = TRUE)
all_ihme_dirs_dates = as.Date(str_extract(all_ihme_dirs, "\\d+_\\d+_\\d+"), format = "%Y_%m_%d")
latest_ihme_update = as.Date("2020-03-30", "%Y-%m-%d")

# Read IHME
x.src.ihme = fread("./source/ihme/Hospitalization_all_locs.csv")
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
  dplyr::mutate(last_update = latest_ihme_update)

x.layer.ihme.geo = x.geo.state %>%
  left_join(x.layer.ihme, by = c('name' = 'location_name')) %>%
  drop_na(icuover_max_needed)

latest_ihme_update_us = as.character(latest_ihme_update, format = '%m_%d_%Y')
write_layer_absolute(x.layer.ihme.geo, paste0("ihme_peak_dates_",latest_ihme_update_us), db =T)
