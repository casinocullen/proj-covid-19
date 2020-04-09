# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# SETUP  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====

require(tidyverse)
require(sf)
require(DBI)
require(data.table)
require(scales)
source("/data/Github/base/functions/write_layer_absolute.R")

# cori-risi connection
#   host: cori-risi
#     DB: data
# Schema: layer

coririsiconf <- config::get("cori-risi", file = "/data/Github/base/config.yml")
coririsi_layer = dbConnect(
  RPostgres::Postgres(),
  user     = coririsiconf$user,
  password = coririsiconf$password,
  dbname   = coririsiconf$dbname,
  host     = coririsiconf$host,
  port     = coririsiconf$port,
  options  =  '-c search_path=sch_layer,public'
)
coririsi_source = dbConnect(
  RPostgres::Postgres(),
  user     = coririsiconf$user,
  password = coririsiconf$password,
  dbname   = coririsiconf$dbname,
  host     = coririsiconf$host,
  port     = coririsiconf$port,
  options  =  '-c search_path=sch_source,public'
)

#dbListTables(coririsi)

rm(coririsiconf)

# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# IHME  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
x.date <- as.character(Sys.Date(), format = '%m-%d-%Y')


# Source Table
# url_to_s3(url = "https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip", 
#           filename = "ihme-covid19.zip", 
#           s3path = "source/proj-covid-19/", 
#           s3bucket =  "cori-layers")

utils::unzip("./source/ihme-covid19.zip", exdir = "./source/ihme/", junkpaths = T, overwrite = T) 

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
  dplyr::mutate(last_update = x.date)

x.layer.ihme.geo = x.geo.state %>%
  left_join(x.layer.ihme, by = c('name' = 'location_name')) %>%
  drop_na(icuover_max_needed)

write_layer_absolute(x.layer.ihme.geo, paste0("ihme_peak_dates_",x.date), db =T)

# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# ATTR Health  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# County geo
x.geo.county = st_read(coririsi_layer, 'geo_attr_county_pg') 

# COVID-19 case
x.layer.case = st_read(coririsi_layer, "covid_19_county_latest") %>% 
  st_drop_geometry() %>% 
  group_by(geoid) %>% 
  dplyr::summarise_all(max) %>% 
  dplyr::select(geoid, covid_19_latest_update = latest_update, confirm, deaths, recovered, active)

# Attr_health
x.layer.county = st_read(coririsi_layer, "acs_county_health") 

# CDC SVI
x.layer.svi = dbReadTable(coririsi_source, "cdc_svi") %>%
  dplyr::select(FIPS, EP_NOVEH, EP_MUNIT, EP_DISABL, RPL_THEME1, RPL_THEMES)

# Hospital drive time
x.hospital.dt = st_read(coririsi_layer, "hospitals_drivetime_cori_pg") %>%
  st_transform(crs = 4269) %>% 
  dplyr::filter(type %in% c('CRITICAL ACCESS', 'GENERAL ACUTE CARE'), 
                status == "OPEN") %>% 
  dplyr::select(-geoid) %>% 
  dplyr::mutate(beds = ifelse(beds == -999, 0, beds),
                hospital_name = name)

x.hospital.dt.county = x.geo.county %>% 
  st_join(x.hospital.dt) %>% 
  dplyr::group_by(geoid) %>% 
  dplyr::summarise(hospitals_name_40_mins = str_c(hospital_name, collapse = ", "),
                   total_estimated_bed_40_mins = sum(beds)) %>% 
  st_drop_geometry()

# Hospital Points
x.hospital.beds = st_read(coririsi_layer, "definitive_healthcare_hospital_beds") %>% 
  dplyr::filter(HOSPITAL_T %in% c('Critical Access Hospital', 'VA Hospital', 'Short Term Acute Care Hospital')) 

x.hospital.beds.county = x.hospital.beds %>% 
  st_drop_geometry() %>%
  group_by(geoid_co) %>% 
  dplyr::summarise(hospital_count =n_distinct(OBJECTID), 
                   hospitals_name = str_c(HOSPITAL_N, collapse = ", "),
                   total_licensed_bed = sum(NUM_LICENS), 
                   total_staffed_bed = sum(NUM_STAFFE), 
                   total_icu_bed = sum(NUM_ICU_BE), 
                   median_bed_utiliz = median(BED_UTILIZ), 
                   potential_increase_beds = sum(Potential_)) %>%
  ungroup()


# ACS
x.layer.acs = dbReadTable(coririsi_layer, "acs_county") %>% 
  dplyr::select(GEOID, unemployed_pct_2018, under_poverty_level_pct_2018, capita_income_past_12_months_2018)

# Staff
x.layer.staff = st_read(coririsi_layer, "hospital_critical_staff_by_county") %>% 
  st_drop_geometry()

# Staff Drivetime
x.layer.staff.dt = st_read(coririsi_layer, "npi_drivetime_40_county") %>% 
  st_drop_geometry()


#IHME
x.layer.ihme = st_read(coririsi_layer, paste0("ihme_peak_dates_",x.date)) %>% 
  st_drop_geometry() %>% 
  dplyr::select(geoid_st,  ends_with("_date"), ends_with("_needed"))


# JOIN ALL TABLES
x.geo.county.join.pg = x.geo.county %>% 
  left_join(x.hospital.dt.county, by = 'geoid') %>%
  left_join(x.hospital.beds.county, by = c('geoid' = 'geoid_co')) %>%
  left_join(x.layer.county, by = 'geoid') %>%
  left_join(x.layer.staff, by = 'geoid') %>%
  left_join(x.layer.staff.dt, by = 'geoid') %>%
  left_join(x.layer.acs, by = c('geoid' = 'GEOID')) %>%
  left_join(x.layer.svi, by = c('geoid' = 'FIPS')) %>% 
  left_join(x.layer.case, by = c('geoid')) %>% 
  left_join(x.layer.ihme, by = c('geoid_st')) %>% 
  dplyr::mutate(pop_density = total_population_2018/land_sqmi, 
                confirm_pct = confirm/total_population_2018 * 100,
                death_pct = deaths/confirm * 100,
                all_beds_40_mins_per_1000 = total_estimated_bed_40_mins * 1000/total_population_2018,
                pct_65_over_2018 = population_65_over_2018/total_population_2018 * 100, 
                pct_85_over_2018 = population_85_over_2018/total_population_2018 * 100,                
                icu_beds_per_1000 = round(total_icu_bed * 10000/total_population_2018)/10,
                icu_beds_per_1000_elder = round(total_icu_bed * 10000/population_65_over_2018)/10,
                icu_beds_per_100k = round(total_icu_bed * 100000/total_population_2018),
                icu_beds_per_100k_elder = round(total_icu_bed * 100000/population_65_over_2018)) %>%
  dplyr::select(geoid, name, namelsad, st_stusps, 
                confirm, deaths, 
                total_staff,
                total_staff_dt,
                hospital_count, total_licensed_bed, total_staffed_bed, total_icu_bed, potential_increase_beds, 
                total_estimated_bed_40_mins, 
                all_beds_40_mins_per_1000, icu_beds_per_1000, icu_beds_per_100k, 
                total_population_2018, pop_density, population_65_over_2018, population_85_over_2018, pct_65_over_2018, pct_85_over_2018,
                pop_over_65_not_covered_by_health_insurance_pct_2018,
                unemployed_pct_2018,                          
                under_poverty_level_pct_2018,                   
                capita_income_past_12_months_2018,
                pct_disability = EP_DISABL,
                pct_no_vehicle = EP_NOVEH, 
                pct_10_more_units_housing = EP_MUNIT, 
                svi_socioeconomic = RPL_THEME1, 
                svi_all = RPL_THEMES, 
                everything())

names(x.geo.county.join.pg)

# write layer
write_layer_absolute(x.geo.county.join.pg, "attr_county_health", db = T)


# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# County preparedness score  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
x.state.pop = dbGetQuery(coririsi_layer, "SELECT geoid, st_total_population_2018 FROM attr_state_full")
x.state.case = dbGetQuery(coririsi_layer, "SELECT st_stusps, sum(confirm) AS st_confirm FROM covid_19_county_latest GROUP BY st_stusps")


x.layer.name <- "attr_county_health"
x.layer.table = st_read(coririsi_layer, x.layer.name) %>% 
  left_join(x.state.pop, by = c('geoid_st' = 'geoid')) %>% 
  dplyr::mutate(confirm = ifelse(is.na(confirm), 0, confirm), 
                svi_socioeconomic = ifelse(svi_socioeconomic == -999, mean(svi_socioeconomic, na.rm = T), svi_socioeconomic), 
                total_estimated_bed_40_mins_100k = total_estimated_bed_40_mins*100000/total_population_2018,
                total_staff_100k = total_staff*100000/total_population_2018,
                total_staff_dt_100k = total_staff_dt*100000/total_population_2018,
                icu_bed_max_needed_100k = icu_bed_max_needed*100000/st_total_population_2018 * (total_population_2018/st_total_population_2018),
                icuover_max_needed_100k = icuover_max_needed*100000/st_total_population_2018 * (total_population_2018/st_total_population_2018)) %>% 
  drop_na(pct_65_over_2018, svi_socioeconomic)

# Beds pillar ----
beds_pillar = x.layer.table[, c("geoid", "name", "st_stusps" ,"total_estimated_bed_40_mins", "total_estimated_bed_40_mins_100k")]
beds_pillar = beds_pillar %>%
  dplyr::mutate(
    total_estimated_bed_40_mins = ifelse(is.na(total_estimated_bed_40_mins), 0, total_estimated_bed_40_mins),
    total_estimated_bed_40_mins_100k = ifelse(is.na(total_estimated_bed_40_mins_100k), 0, total_estimated_bed_40_mins_100k),
    # Composite Index 1 uses percentile
    bed_score_1 = ntile(total_estimated_bed_40_mins_100k, 100),
    # Composite Index 2 uses rescale
    bed_score_2 = as.integer(rescale(total_estimated_bed_40_mins_100k, to = c(0, 100))))

# Staff pillar ----
staff_pillar = x.layer.table[, c("geoid", "name", "st_stusps", 'total_staff', "total_staff_dt", 'total_staff_100k','total_staff_dt_100k')]
staff_pillar = staff_pillar %>%
  dplyr::mutate(    
    total_staff_dt_100k = ifelse(is.na(total_staff_dt_100k), 0, total_staff_dt_100k),
    # Composite Index 1 uses percentile
    staff_score_1 = ntile(total_staff_dt_100k, 100),
    staff_score_2 = ntile(total_staff_100k, 100))
    

# Demograhic pillar ----
dem_pillar = x.layer.table[, c("geoid", "name", "st_stusps" ,"pct_65_over_2018")]
dem_pillar = dem_pillar %>%
  dplyr::mutate(# Composite Index 1 uses percentile
    dem_score_1 = ntile(-pct_65_over_2018, 100),
    # Composite Index 2 uses rescale
    dem_score_2 = as.integer(rescale(-pct_65_over_2018, to = c(0, 100))))

# ICU beds shortage pillar ----
proj_pillar = x.layer.table[, c("geoid", "name", "st_stusps" , 'icu_bed_max_needed', 'icu_bed_max_needed_100k', 'icuover_max_needed', "icuover_max_needed_100k")]
proj_pillar = proj_pillar %>%
  dplyr::mutate(# Composite Index 1 uses percentile
    proj_score_1 = ntile(-icuover_max_needed_100k, 100),
    # Composite Index 2 uses rescale
    proj_score_2 = as.integer(rescale(-icuover_max_needed_100k, to = c(0, 100))))

# Social-economic pillar ----
se_pillar = x.layer.table[, c("geoid", "name", "st_stusps" ,"svi_socioeconomic")]
se_pillar = se_pillar %>%
  dplyr::mutate(# Composite Index 1 uses percentile
    se_score_1 = ntile(-svi_socioeconomic, 100))

# Combine all
all_index = x.layer.table[, c('geoid', 'name', 'st_stusps', 'icuover_max_date', 'icu_bed_max_date')] %>% 
  left_join(beds_pillar %>% data.frame %>% dplyr::select(-geom) , by= c('geoid', 'name', 'st_stusps')) %>%
  left_join(staff_pillar %>% data.frame %>% dplyr::select(-geom) , by= c('geoid', 'name', 'st_stusps')) %>%
  left_join(dem_pillar %>% data.frame %>% dplyr::select(-geom), by= c('geoid', 'name', 'st_stusps')) %>%
  left_join(se_pillar %>% data.frame %>% dplyr::select(-geom), by= c('geoid', 'name', 'st_stusps')) %>% 
  left_join(proj_pillar %>% data.frame %>% dplyr::select(-geom), by= c('geoid', 'name', 'st_stusps')) %>% 
  dplyr::mutate(prep_score = ( bed_score_1 + staff_score_1 + dem_score_1 + se_score_1 + proj_score_1) / 5,
                prep_score_old = ( bed_score_1 + staff_score_2 + dem_score_1 + se_score_1 + proj_score_1) / 5,
                prep_score_old = ntile(prep_score_old, 100),
                prep_score_1 = ntile(prep_score, 100),
                prep_score_2 = as.integer(rescale(prep_score, to = c(0, 100))),
                prep_level = case_when(prep_score_1 <= 20 ~ "Very Low", 
                                       prep_score_1 > 20 & prep_score_1 <= 40 ~ "Low", 
                                       prep_score_1 > 40 & prep_score_1 <= 60 ~ "Medium", 
                                       prep_score_1 > 60 & prep_score_1 <= 80 ~ "High", 
                                       prep_score_1 > 80 ~ "Very High"),
                last_update = x.date) %>% 
  dplyr::mutate(svi_socioeconomic = round(svi_socioeconomic * 100)/100,
                pct_65_over_2018 = round(pct_65_over_2018 * 10)/10,
                total_estimated_bed_40_mins_100k = round(total_estimated_bed_40_mins_100k),
                total_staff_dt_100k = round(total_staff_dt_100k),
                icuover_max_needed_100k = round(icuover_max_needed_100k * 100)/100)

names(all_index)
write_layer_absolute(all_index, layer.name = "county_preparedness_score", layer.type="pg", db=T, fs=T, new.server = T, new.server.overwrite = T)



# Shareable version
all_index_share = all_index %>%
  dplyr::select(geoid,
                name,
                st_stusps,
                prep_score = prep_score_1,
                prep_level,
                icuover_max_date,
                icu_bed_max_date,
                pc_score = bed_score_1,
                total_estimated_bed_40_mins,
                total_estimated_bed_40_mins_100k,
                hr_score = staff_score_1,
                total_staff_dt,
                total_staff_dt_100k,
                dem_score = dem_score_1,
                pct_65_over_2018,
                se_score = se_score_1,
                svi_socioeconomic, 
                covid_score = proj_score_1,
                icuover_max_needed,
                icuover_max_needed_100k,
                last_ihme_update = last_update)

write_layer_absolute(all_index_share, layer.name = "county_preparedness_score_v0_3", layer.type="pg", db=T, fs=T, new.server = T, new.server.overwrite = T)


rm(list = ls())
gc()