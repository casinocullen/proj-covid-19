source("../base/global.R")
x.date <- as.character(Sys.Date(), format = '%Y%m%d')

x.state.pop = dbGetQuery(coririsi_layer, "SELECT geoid, st_total_population_2018 FROM attr_state_full")

x.layer.name <- "attr_county_health"
x.layer.table = st_read(coririsi_layer, x.layer.name) %>% 
  left_join(x.state.pop, by = c('geoid_st' = 'geoid')) %>% 
  dplyr::mutate(svi_socioeconomic = ifelse(svi_socioeconomic == -999, mean(svi_socioeconomic, na.rm = T), svi_socioeconomic), 
                total_estimated_bed_40_mins_100k = total_estimated_bed_40_mins*100000/total_population_2018,
                total_staff_dt_100k = total_staff_dt*100000/total_population_2018,
                total_staff_100k = total_staff*100000/total_population_2018,
                icu_bed_max_needed_100k = icu_bed_max_needed*100000/st_total_population_2018 * (total_population_2018/st_total_population_2018),
                icuover_max_needed_100k = icuover_max_needed*100000/st_total_population_2018 * (total_population_2018/st_total_population_2018)) %>% 
  drop_na(pct_65_over_2018, svi_socioeconomic)

names(x.layer.table)


beds_pillar = x.layer.table[, c("geoid", "name", "st_stusps" ,"total_estimated_bed_40_mins", "total_estimated_bed_40_mins_100k")]
beds_pillar = beds_pillar %>%
  dplyr::mutate(
    total_estimated_bed_40_mins_100k = ifelse(is.na(total_estimated_bed_40_mins_100k), 0, total_estimated_bed_40_mins_100k),
    # Composite Index 1 uses percentile
    bed_score_1 = ntile(total_estimated_bed_40_mins_100k, 100),
    # Composite Index 2 uses rescale
    bed_score_2 = as.integer(rescale(total_estimated_bed_40_mins_100k, to = c(0, 100))))

staff_pillar = x.layer.table[, c("geoid", "name", "st_stusps" ,"total_staff", "total_staff_dt","total_staff_100k", 'total_staff_dt_100k')]
staff_pillar = staff_pillar %>%
  dplyr::mutate(    
    total_staff_100k = ifelse(is.na(total_staff_100k), 0, total_staff_100k),
    total_staff_dt_100k = ifelse(is.na(total_staff_dt_100k), 0, total_staff_dt_100k),
    # Composite Index 1 uses percentile
    staff_score_1 = ntile(total_staff_100k, 100),
    staff_score_3 = ntile(total_staff_dt_100k, 100),
    # Composite Index 2 uses rescale
    staff_score_2 = as.integer(rescale(total_staff_100k, to = c(0, 100))))


dem_pillar = x.layer.table[, c("geoid", "name", "st_stusps" ,"pct_65_over_2018")]
dem_pillar = dem_pillar %>%
  dplyr::mutate(# Composite Index 1 uses percentile
    dem_score_1 = ntile(-pct_65_over_2018, 100),
    # Composite Index 2 uses rescale
    dem_score_2 = as.integer(rescale(-pct_65_over_2018, to = c(0, 100))))


proj_pillar = x.layer.table[, c("geoid", "name", "st_stusps" , "icuover_max_needed_100k", "icu_bed_max_needed", "icu_bed_max_needed_100k")]
proj_pillar = proj_pillar %>%
  dplyr::mutate(# Composite Index 1 uses percentile
    proj_score_1 = ntile(-icuover_max_needed_100k, 100),
    # Composite Index 2 uses rescale
    proj_score_2 = as.integer(rescale(-icuover_max_needed_100k, to = c(0, 100))))


se_pillar = x.layer.table[, c("geoid", "name", "st_stusps" ,"svi_socioeconomic")]
se_pillar = se_pillar %>%
  dplyr::mutate(# Composite Index 1 uses percentile
    se_score_1 = ntile(-svi_socioeconomic, 100))


all_index = x.layer.table[, c('geoid', 'name', 'st_stusps', 'icuover_max_date', 'icu_bed_max_date')] %>% 
  left_join(beds_pillar %>% data.frame %>% dplyr::select(-geom) , by= c('geoid', 'name', 'st_stusps')) %>%
  left_join(staff_pillar %>% data.frame %>% dplyr::select(-geom) , by= c('geoid', 'name', 'st_stusps')) %>%
  left_join(dem_pillar %>% data.frame %>% dplyr::select(-geom), by= c('geoid', 'name', 'st_stusps')) %>%
  left_join(se_pillar %>% data.frame %>% dplyr::select(-geom), by= c('geoid', 'name', 'st_stusps')) %>% 
  left_join(proj_pillar %>% data.frame %>% dplyr::select(-geom), by= c('geoid', 'name', 'st_stusps')) %>% 
  dplyr::mutate(prep_score = ( bed_score_1 + staff_score_1 + dem_score_1 + se_score_1 + proj_score_1) / 5,
                prep_score_dt = ( bed_score_1 + staff_score_3 + dem_score_1 + se_score_1 + proj_score_1) / 5,
                prep_score_1 = ntile(prep_score, 100),
                prep_score_3 = ntile(prep_score_dt, 100),
                prep_score_diff = prep_score_3-prep_score_1,
                prep_score_2 = as.integer(rescale(prep_score, to = c(0, 100)))) %>% 
  dplyr::mutate(prep_level = case_when(prep_score_1 <= 20 ~ "Extremely Low Preparedness", 
                                       prep_score_1 > 20 & prep_score_1 <= 40 ~ "Low Preparedness", 
                                       prep_score_1 > 40 & prep_score_1 <= 60 ~ "Medium Preparedness", 
                                       prep_score_1 > 60 & prep_score_1 <= 80 ~ "High Preparedness", 
                                       prep_score_1 > 80 ~ "Well-prepared")) %>% 
  dplyr::mutate(svi_socioeconomic = round(svi_socioeconomic * 100)/100,
                pct_65_over_2018 = round(pct_65_over_2018 * 10)/10,
                total_estimated_bed_40_mins_100k = round(total_estimated_bed_40_mins_100k),
                total_staff_100k = round(total_staff_100k),
                total_staff_dt_100k = round(total_staff_dt_100k),
                icuover_max_needed_100k = round(icuover_max_needed_100k * 100)/100,
                icu_bed_max_needed_100k = round(icu_bed_max_needed_100k * 100)/100)


write_layer(all_index, layer.name = "county_preparedness_score_temp", layer.type="pg", new.server = T, new.server.overwrite = T)


write_layer(all_index, layer.name = "county_preparedness_score", layer.type="pg", db=T, fs=T, ngacarto=T, ngacarto.overwrite = T, new.server = T, new.server.overwrite = T)



