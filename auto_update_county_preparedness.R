# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# SETUP  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====

require(tidyverse)
require(sf)
require(DBI)
require(data.table)
require(scales)
require(tools)
require(aws.s3)
source("/data/Github/base/functions/write_layer_absolute.R")
source("/data/Github/base/functions/url_to_s3.R")

# cori-risi connection
#   host: cori-risi
#     DB: data
# Schema: layer

coririsiconf <- config::get("cori-risi", file = "/data/Github/base/config.yml")
cartoconf <- config::get("cartoapi", file = "/data/Github/base/config.yml")
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


# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# IHME  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
x.date <- as.character(Sys.Date(), format = '%m-%d-%Y')


# Source Table
url = "https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip"

# Get source file
file.remove("./source/ihme-covid19.zip")
curl::curl_download(url, "./source/ihme-covid19.zip")


utils::unzip("./source/ihme-covid19.zip", exdir = "./source/ihme/", junkpaths = T, overwrite = T) 
utils::unzip("./source/ihme-covid19.zip", exdir = "./source/ihme/", overwrite = T) 

# Find the latest ihme update
all_ihme_dirs = list.dirs(path = "./source/ihme/", full.names = TRUE, recursive = TRUE)
all_ihme_dirs_dates = as.Date(str_extract(all_ihme_dirs, "\\d+_\\d+_\\d+"), format = "%Y_%m_%d")

latest_ihme_update = max(all_ihme_dirs_dates, na.rm = T)

# Read IHME
x.src.ihme = fread("./source/ihme/Hospitalization_all_locs.csv")
x.geo.state = st_read(coririsi_layer, 'geo_attr_state_pg')

# Create all the _max_needed and _max_date field we want
# The source has one record per model day per location (state/country) so we have to group_by and summarize to get one record per location
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
  # Just keep the fields we created and the location name
  dplyr::select(location_name, ends_with("_date"), ends_with("_needed")) %>% 
  janitor::clean_names() %>%
  # Add latest_ihme_update that we determined before
  dplyr::mutate(last_update = latest_ihme_update)

# Remove records that are not a U.S. State. (Countries)
x.layer.ihme.geo = x.geo.state %>%
  left_join(x.layer.ihme, by = c('name' = 'location_name')) %>%
  drop_na(icuover_max_needed)

# Write resulting table to our database with date stamp, so we have historical numbers
latest_ihme_update_us = as.character(latest_ihme_update, format = '%m_%d_%Y')
write_layer_absolute(x.layer.ihme.geo, paste0("ihme_peak_dates_",latest_ihme_update_us), db =T)

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

# Get hospital drive times that overlap with counties 
x.hospital.dt.county = x.geo.county %>% 
  st_join(x.hospital.dt) %>% 
  dplyr::group_by(geoid) %>% 
  dplyr::summarise(hospitals_name_40_mins = str_c(hospital_name, collapse = ", "),
                   total_estimated_bed_40_mins = sum(beds)) %>% 
  st_drop_geometry()

# Get Hospital Points and attach county data
x.hospital.beds = st_read(coririsi_layer, "definitive_healthcare_hospital_beds") %>% 
  dplyr::filter(hospital_t %in% c('Critical Access Hospital', 'VA Hospital', 'Short Term Acute Care Hospital')) %>% 
  st_join(x.geo.county)

# Aggregate hospital data by county
x.hospital.beds.county = x.hospital.beds %>% 
  st_drop_geometry() %>%
  group_by(geoid) %>% 
  dplyr::summarise(hospital_count =n_distinct(objectid), 
                   hospitals_name = str_c(hospital_n, collapse = ", "),
                   total_licensed_bed = sum(num_licens), 
                   total_staffed_bed = sum(num_staffe), 
                   total_icu_bed = sum(num_icu_be), 
                   median_bed_utiliz = median(bed_utiliz), 
                   potential_increase_beds = sum(potential)) %>%
    ungroup()


# Get ACS fields
x.layer.acs = dbReadTable(coririsi_layer, "acs_county") %>% 
  dplyr::select(GEOID, unemployed_pct_2018, under_poverty_level_pct_2018, capita_income_past_12_months_2018)

# Get hospital staff data
x.layer.staff = st_read(coririsi_layer, "hospital_critical_staff_by_county") %>% 
  st_drop_geometry()

# Get Staff Drivetime
x.layer.staff.dt = st_read(coririsi_layer, "npi_drivetime_40_county") %>% 
  st_drop_geometry()


# Get IHME data
x.layer.ihme = st_read(coririsi_layer, paste0("ihme_peak_dates_",latest_ihme_update_us)) %>% 
  st_drop_geometry() %>% 
  dplyr::select(geoid_st,  ends_with("_date"), ends_with("_needed"))


# JOIN ALL TABLES
x.geo.county.join.pg = x.geo.county %>% #26
  left_join(x.hospital.dt.county, by = 'geoid') %>% #2
  left_join(x.hospital.beds.county, by = c('geoid' = 'geoid')) %>% #7
  left_join(x.layer.county, by = 'geoid') %>% #8
  left_join(x.layer.staff, by = 'geoid') %>% #6
  left_join(x.layer.staff.dt, by = 'geoid') %>% #6
  left_join(x.layer.acs, by = c('geoid' = 'GEOID')) %>% #3
  left_join(x.layer.svi, by = c('geoid' = 'FIPS')) %>% #5
  left_join(x.layer.case, by = c('geoid')) %>% #5
  left_join(x.layer.ihme, by = c('geoid_st')) %>% #12
  dplyr::mutate(pop_density = total_population_2018/land_sqmi, 
                confirm_pct = confirm/total_population_2018 * 100,
                death_pct = deaths/confirm * 100,
                all_beds_40_mins_per_1000 = total_estimated_bed_40_mins * 1000/total_population_2018,
                pct_65_over_2018 = population_65_over_2018/total_population_2018 * 100, 
                pct_85_over_2018 = population_85_over_2018/total_population_2018 * 100,
                #TODO: Why not use the `digits` option for `round`. I think it would be more clear what is happening.
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
  # Change NA to zero
  dplyr::mutate(confirm = ifelse(is.na(confirm), 0, confirm), 
                #TODO: Not 100% sure what this is doing with -999 values
                svi_socioeconomic = ifelse(svi_socioeconomic == -999, mean(svi_socioeconomic, na.rm = T), svi_socioeconomic), 
                #TODO: Why are we calculating this again here instead of using `all_beds_40_mins_per_1000`
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
    #TODO: should we be dropping these instead?
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
# Using `-` reverse the scale here because we want bigger numbers to be better
dem_pillar = x.layer.table[, c("geoid", "name", "st_stusps" ,"pct_65_over_2018")]
dem_pillar = dem_pillar %>%
  dplyr::mutate(# Composite Index 1 uses percentile
    dem_score_1 = ntile(-pct_65_over_2018, 100),
    # Composite Index 2 uses rescale
    dem_score_2 = as.integer(rescale(-pct_65_over_2018, to = c(0, 100))))

# ICU beds shortage pillar ----
# Using `-` reverse the scale here because we want bigger numbers to be better
proj_pillar = x.layer.table[, c("geoid", "name", "st_stusps" , 'icu_bed_max_needed', 'icu_bed_max_needed_100k', 'icuover_max_needed', "icuover_max_needed_100k")]
proj_pillar = proj_pillar %>%
  dplyr::mutate(# Composite Index 1 uses percentile
    # This is the older calculation, used in prep_score_old
    proj_score_1 = ntile(-icu_bed_max_needed_100k, 100),
    # Composite Index 2 uses rescale, using in prep_score.
    # This is the newer calculation
    proj_score_2 = as.integer(rescale(-icuover_max_needed_100k, to = c(0, 100))))
    #proj_score_2 = as.integer(rescale(-icu_bed_max_needed_100k, to = c(0, 100))))
    #proj_score_2 = ntile(-icuover_max_needed_100k, 100))

# Social-economic pillar ----
# Using `-` reverse the scale here because we want bigger numbers to be better
se_pillar = x.layer.table[, c("geoid", "name", "st_stusps" ,"svi_socioeconomic")]
se_pillar = se_pillar %>%
  dplyr::mutate(# Composite Index 1 uses percentile
    se_score_1 = ntile(-svi_socioeconomic, 100))

# Combine all
all_index = x.layer.table[, c('geoid', 'name', 'st_stusps','lat','lon', 'icuover_max_date', 'icu_bed_max_date')] %>% 
  #TODO: Why are we joining by 'geoid', 'name', and 'st_stusps'?
  left_join(beds_pillar %>% data.frame %>% dplyr::select(-geom) , by= c('geoid', 'name', 'st_stusps')) %>%
  left_join(staff_pillar %>% data.frame %>% dplyr::select(-geom) , by= c('geoid', 'name', 'st_stusps')) %>%
  left_join(dem_pillar %>% data.frame %>% dplyr::select(-geom), by= c('geoid', 'name', 'st_stusps')) %>%
  left_join(se_pillar %>% data.frame %>% dplyr::select(-geom), by= c('geoid', 'name', 'st_stusps')) %>% 
  left_join(proj_pillar %>% data.frame %>% dplyr::select(-geom), by= c('geoid', 'name', 'st_stusps')) %>% 
  #TODO: could we change the name of `prep_score` here so we don't confuse it with the variable of the same name in `all_index_share`
  dplyr::mutate(prep_score = ( bed_score_1 + staff_score_1 + dem_score_1 + se_score_1 + proj_score_1) / 5,
                prep_score_old = ( bed_score_1 + staff_score_1 + dem_score_1 + se_score_1 + proj_score_2) / 5,
                prep_score_1 = ntile(prep_score, 100),
                prep_score_2 = ntile(prep_score_old, 100),
                prep_level = case_when(prep_score_1 <= 20 ~ "Very Low", 
                                       prep_score_1 > 20 & prep_score_1 <= 40 ~ "Low", 
                                       prep_score_1 > 40 & prep_score_1 <= 60 ~ "Medium", 
                                       prep_score_1 > 60 & prep_score_1 <= 80 ~ "High", 
                                       prep_score_1 > 80 ~ "Very High"),
                last_update = latest_ihme_update) %>% 
  dplyr::mutate(svi_socioeconomic = round(svi_socioeconomic * 100)/100,
                pct_65_over_2018 = round(pct_65_over_2018 * 10)/10,
                total_estimated_bed_40_mins_100k = round(total_estimated_bed_40_mins_100k),
                total_staff_dt_100k = round(total_staff_dt_100k),
                icuover_max_needed_100k = round(icuover_max_needed_100k * 100)/100,
                lat = as.numeric(lat), 
                lon = as.numeric(lon))

names(all_index)
write_layer_absolute(all_index, layer.name = "county_preparedness_score", layer.type="pg", db=T, fs=T, new.server = T, new.server.overwrite = T)


# Shareable version
all_index_share = all_index %>%
  dplyr::select(geoid,
                name,
                st_stusps,
                lat,
                lon,
                prep_score = prep_score_1,
                prep_score_alt = prep_score_2,
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
                icu_bed_max_needed, 
                icu_bed_max_needed_100k,
                last_ihme_update = last_update)

names(all_index_share)

write_layer_absolute(all_index_share, layer.name = "county_preparedness_score_v0_5", layer.type="pg", db=T, fs=T, new.server = T, new.server.overwrite = T)

# State Average
all_index_share_st = all_index_share %>% 
  st_drop_geometry() %>% 
  group_by(st_stusps) %>% 
  dplyr::summarise(prep_score = mean(prep_score, na.rm = T), 
                   pc_score = mean(pc_score, na.rm = T), 
                   total_estimated_bed_40_mins = mean(total_estimated_bed_40_mins, na.rm = T), 
                   total_estimated_bed_40_mins_100k = mean(total_estimated_bed_40_mins_100k, na.rm = T), 
                   hr_score = mean(hr_score, na.rm = T), 
                   total_staff_dt = mean(total_staff_dt, na.rm = T), 
                   total_staff_dt_100k = mean(total_staff_dt_100k, na.rm = T), 
                   dem_score = mean(dem_score, na.rm = T), 
                   pct_65_over_2018 = mean(pct_65_over_2018, na.rm = T), 
                   se_score = mean(se_score, na.rm = T), 
                   svi_socioeconomic = mean(svi_socioeconomic, na.rm = T), 
                   covid_score = mean(covid_score, na.rm = T), 
                   icuover_max_needed = mean(icuover_max_needed, na.rm = T), 
                   icuover_max_needed_100k = mean(icuover_max_needed_100k, na.rm = T),
                   #TODO: I like this better than `max` but still wish thare was an function that checked to make sure all values were the same with a clear name.
                   last_ihme_update = first(last_ihme_update)) %>% 
  ungroup()

all_index_share_us = all_index_share %>% 
  st_drop_geometry() %>% 
  dplyr::mutate(country = "United States") %>%
  group_by(country) %>% 
  dplyr::summarise(prep_score = mean(prep_score, na.rm = T), 
                   pc_score = mean(pc_score, na.rm = T), 
                   total_estimated_bed_40_mins = mean(total_estimated_bed_40_mins, na.rm = T), 
                   total_estimated_bed_40_mins_100k = mean(total_estimated_bed_40_mins_100k, na.rm = T), 
                   hr_score = mean(hr_score, na.rm = T), 
                   total_staff_dt = mean(total_staff_dt, na.rm = T), 
                   total_staff_dt_100k = mean(total_staff_dt_100k, na.rm = T), 
                   dem_score = mean(dem_score, na.rm = T), 
                   pct_65_over_2018 = mean(pct_65_over_2018, na.rm = T), 
                   se_score = mean(se_score, na.rm = T), 
                   svi_socioeconomic = mean(svi_socioeconomic, na.rm = T), 
                   covid_score = mean(covid_score, na.rm = T), 
                   icuover_max_needed = mean(icuover_max_needed, na.rm = T), 
                   icuover_max_needed_100k = mean(icuover_max_needed_100k, na.rm = T), 
                   last_ihme_update = first(last_ihme_update)) %>% 
  ungroup()


path_st = paste0("/data/Github/proj-covid-19/output/county_preparedness_score_state_avg.csv")
write.csv(all_index_share_st, path_st, row.names = F)
system(paste0("curl -v -F file=@'",path_st,"' 'https://ruralinnovation.carto.com/u/ruralinnovation-admin/api/v1/imports/?api_key=", cartoconf$newcarto,"&privacy=link", "&collision_strategy=overwrite","'"))

path_us = paste0("/data/Github/proj-covid-19/output/county_preparedness_score_us_avg.csv")
write.csv(all_index_share_us, path_us, row.names = F)
system(paste0("curl -v -F file=@'",path_us,"' 'https://ruralinnovation.carto.com/u/ruralinnovation-admin/api/v1/imports/?api_key=", cartoconf$newcarto,"&privacy=link", "&collision_strategy=overwrite","'"))


rm(list = ls())
gc()