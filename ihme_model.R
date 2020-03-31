# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# IHME  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))


source("../base/global.R")

# Source Table
x.src.ihme = fread("./source/hospitalization_all_locs_corrected.csv")

dbWriteTable(coririsi_source, "ihme_state_projection", x.src.ihme)


# Layer table
x.geo.state = st_read(coririsi_layer, 'geo_attr_state_pg')

x.layer.ihme = x.src.ihme %>% 
  dplyr::mutate(date_reported = as.Date(date_reported, format = "%Y-%m-%d")) %>% 
  group_by(location_name) %>% 
  dplyr::mutate(ICUbed_max_date = case_when(max(ICUbed_mean) == ICUbed_mean ~ date_reported),
                ICUbed_max_needed = case_when(max(ICUbed_mean) == ICUbed_mean ~ ICUbed_mean),
                allbed_max_date = case_when(max(allbed_mean) == allbed_mean ~ date_reported),
                allbed_max_needed = case_when(max(allbed_mean) == allbed_mean ~ allbed_mean),
                InvVen_max_date = case_when(max(InvVen_mean) == InvVen_mean ~ date_reported),
                InvVen_max_needed = case_when(max(InvVen_mean) == InvVen_mean ~ InvVen_mean),
                newICU_max_date = case_when(max(newICU_mean) == newICU_mean ~ date_reported),
                newICU_max_needed = case_when(max(newICU_mean) == newICU_mean ~ newICU_mean),
                admis_max_date = case_when(max(admis_mean) == admis_mean ~ date_reported),
                admis_max_needed = case_when(max(admis_mean) == admis_mean ~ admis_mean),
                icuover_max_date = case_when(max(icuover_mean) == icuover_mean ~ date_reported),
                icuover_max_needed = case_when(max(icuover_mean) == icuover_mean ~ icuover_mean)) %>% 
  ungroup() %>% 
  group_by(location_name) %>% 
  dplyr::summarise_all(max) %>% 
  dplyr::select(location_name, ends_with("_date"), ends_with("_needed")) %>% 
  janitor::clean_names()

x.layer.ihme.geo = x.geo.state %>%
  left_join(x.layer.ihme, by = c('name' = 'location_name')) %>%
  drop_na(icuover_max_needed)

write_layer(x.layer.ihme.geo, "ihme_peak_dates", db =T , fs = T, ngacarto = T, new.server = T)
