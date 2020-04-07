# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Attr health county  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))


source("../base/global.R")
x.date <- as.character(Sys.Date(), format = '%m_%d_%Y')

# County geo
x.geo.county = st_read(coririsi_layer, 'geo_attr_county_pg') 

# COVID-19 case
x.layer.case = st_read(coririsi_layer, "covid_19_county_latest") %>% 
  st_drop_geometry() %>% 
  group_by(geoid) %>% 
  dplyr::summarise_all(max) %>% 
  dplyr::select(geoid, covid_19_latest_update = latest_update, confirm, deaths, recovered, active)

names(x.layer.case)

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
write_layer(x.geo.county.join.pg, "attr_county_health", db = T, new.server = T, new.server.overwrite = T, ngacarto = T, ngacarto.overwrite = T) 






#######################################
############### OLD ###################
#######################################
x.layer.attr.num = x.layer.attr %>% 
  dplyr::select_if(is.numeric) %>% 
  drop_na()

x.layer.attr.num.cor = cor(x.layer.attr.num, x.layer.attr.num$death_pct) %>% 
  as.data.frame() %>% 
  rownames_to_column()%>%
  dplyr::arrange(-V1)



x.layer.county.time = st_read(coririsi_layer, "covid_19_county_03_30_2020") %>% 
  st_drop_geometry() %>% 
  dplyr::select(starts_with('confirm'))

x.layer.county.time.test = x.layer.county.time[1287,] %>% 
  gather() 

rownames(x.layer.county.time.test) <- NULL
x.layer.county.time.test = x.layer.county.time.test %>% 
  rownames_to_column(var = "day") %>% 
  as.data.frame() %>% 
  dplyr::mutate(value = coalesce.na(value, 0),
                day = as.numeric(day))
x.layer.county.time.test$accel <- c(NA, with(x.layer.county.time.test, diff(value)/1))


x = x.layer.county.time.test$day
y = x.layer.county.time.test$accel
y.con = x.layer.county.time.test$value

plot(y = y,x = x)
plot(y = y.con,x = x)

#fit first degree polynomial equation:
fit  <- lm(y~x)
#second degree
fit2 <- lm(y~poly(x,2,raw=TRUE))
#third degree
fit3 <- lm(y~poly(x,3,raw=TRUE))
#fourth degree
fit4 <- lm(y~poly(x,4,raw=TRUE))


#fit first degree polynomial equation:
fit  <- lm(y.con~x)
#second degree
fit2 <- lm(y.con~poly(x,2,raw=TRUE))
#third degree
fit3 <- lm(y.con~poly(x,3,raw=TRUE))
#fourth degree
fit4 <- lm(y.con~poly(x,4,raw=TRUE))


summary(fit)
summary(fit2)
summary(fit3)
summary(fit4)

plot(fit3)


model <- lm(y ~ x + I(x^2) + I(x^3))
summary(model)
ggplotRegression(model)

fits <- model$fitted.values



xx = -5:20
yy = -5.1071 + 5.7143*xx + -0.3571 * xx^2
plot(y = yy, x = xx)
plot(y = x.layer.county.time.test$accel, x = x.layer.county.time.test$day)


ylog = log(x.layer.county.time.test$value)

plot(y = ylog, x = x)
summary(fit)



