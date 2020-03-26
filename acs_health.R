# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Health ACS Vars ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))

source("../tidycensus/tidycencus-functions.R")
source("../tidycensus/.Rprofile")

source("../base/global.R")


x.codebook <- tidycencusFields("health")

# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# Variables ----

# Variables from codebook that will be renamed
x.var.rename <- x.codebook %>%
  dplyr::filter(Destiny == "rename")

# Variables from codebook that be used as sources for calculated fields and then deleted
x.var.source <- x.codebook %>%
  dplyr::filter(Destiny == "source-delete")

# Variables from codebook that have formulas ready to go
x.var.formula <- x.codebook %>%
  dplyr::filter(Destiny == "formula")

# Variables from codebook that will need formulas built in R
x.var.calc <- x.codebook %>%
  dplyr::filter(Destiny == "calculate")

# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# County ----
## Table
x.table.county <- tidycencusTable("county")
x.table.county.cori <- tidycencusCORI(x.table.county) %>%
  dplyr::select(geoid = GEOID, ends_with("2018"))

dbWriteTable(coririsi_layer, "acs_county_health", x.table.county.cori, overwrite = T)


# ZCTA ----
## Table
x.table.zcta <- tidycencusTable("zcta")
x.table.zcta.cori <- tidycencusCORI(x.table.zcta) %>%
  dplyr::select(geoid = GEOID, ends_with("2018"))

dbWriteTable(coririsi_layer, "acs_zcta_health", x.table.zcta.cori, overwrite = T)
#x.table.zcta.cori = dbReadTable(coririsi_layer, "acs_zcta_health")


# HRR ----
## Table
x.src.hsa.zip.xw = dbReadTable(coririsi_source, "hrr_hsa_zipcode_crosswalk") %>% 
  dplyr::mutate(hsanum = as.character(hsanum), 
                hrrnum = as.character(hrrnum))



# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Hospitals ----
x.hospital.cori = st_read(coririsi_layer, "hospitals_cori_pt")  %>% 
  dplyr::filter(type %in% c('CRITICAL ACCESS', 'GENERAL ACUTE CARE'), 
                status == "OPEN") %>% 
  dplyr::select(-geoid) %>% 
  dplyr::mutate(beds = ifelse(beds == -999, 0, beds),
                hospital_name = name)

x.hospital.beds = st_read(coririsi_layer, "definitive_healthcare_hospital_beds") %>% 
  dplyr::filter(HOSPITAL_T %in% c('Critical Access Hospital', 'VA Hospital', 'Short Term Acute Care Hospital')) 
  

x.summary = GetCodebookFieldValues(x.hospital.beds %>% data.frame(), x.max.value.cnt = 40)

names(x.hospital.beds)
names(x.hospital.cori)

# County
x.hospital.beds.county = x.hospital.beds %>% 
  st_drop_geometry() %>%
  group_by(geoid_co) %>% 
  dplyr::summarise(hospital_count =n(OBJECTID), 
                   hospitals_name = str_c(HOSPITAL_N, collapse = ", "),
                   total_licensed_bed = sum(NUM_LICENS), 
                   total_staffed_bed = sum(NUM_STAFFE), 
                   total_icu_bed = sum(NUM_ICU_BE), 
                   median_bed_utiliz = median(BED_UTILIZ), 
                   potential_increase_beds = sum(Potential_)) %>%
  ungroup()

x.hospital.cori.county = x.hospital.cori %>% 
  st_drop_geometry() %>%
  group_by(geoid_co) %>% 
  dplyr::summarise(cori_hospital_count = n(hosid),
                   cori_hospital_name = str_c(hospital_name, collapse = ", "),
                   total_cori_hospital_beds = sum(beds)) %>%
  ungroup()
  
# ZCTA
x.hospital.beds.zcta = x.hospital.beds %>% 
  st_drop_geometry() %>%
  group_by(geoid_zc) %>% 
  dplyr::summarise(hospital_count =n(OBJECTID), 
                   hospitals_name = str_c(HOSPITAL_N, collapse = ", "),
                   total_licensed_bed = sum(NUM_LICENS), 
                   total_staffed_bed = sum(NUM_STAFFE), 
                   total_icu_bed = sum(NUM_ICU_BE), 
                   median_bed_utiliz = median(BED_UTILIZ), 
                   potential_increase_beds = sum(Potential_)) %>%
  ungroup()

x.hospital.cori.zcta = x.hospital.cori %>% 
  st_drop_geometry() %>%
  group_by(geoid_zc) %>% 
  dplyr::summarise(cori_hospital_count = n(hosid),
                   cori_hospital_name = str_c(hospital_name, collapse = ", "),
                   total_cori_hospital_beds = sum(beds)) %>%
  ungroup()

# HRR
x.hospital.beds.hrr = x.hospital.beds %>% 
  st_drop_geometry() %>%
  group_by(geoid_hrr) %>% 
  dplyr::summarise(hospital_count =n(OBJECTID), 
                   hospitals_name = str_c(HOSPITAL_N, collapse = ", "),
                   total_licensed_bed = sum(NUM_LICENS), 
                   total_staffed_bed = sum(NUM_STAFFE), 
                   total_icu_bed = sum(NUM_ICU_BE), 
                   median_bed_utiliz = median(BED_UTILIZ), 
                   potential_increase_beds = sum(Potential_)) %>%
  ungroup()

x.hospital.cori.hrr = x.hospital.cori %>% 
  st_drop_geometry() %>%
  group_by(geoid_hrr) %>% 
  dplyr::summarise(cori_hospital_count = n(hosid),
                   cori_hospital_name = str_c(hospital_name, collapse = ", "),
                   total_cori_hospital_beds = sum(beds)) %>%
  ungroup()

# HSA
x.hospital.beds.hsa = x.hospital.beds %>% 
  st_drop_geometry() %>%
  group_by(geoid_hsa) %>% 
  dplyr::summarise(hospital_count =n(OBJECTID), 
                   hospitals_name = str_c(HOSPITAL_N, collapse = ", "),
                   total_licensed_bed = sum(NUM_LICENS), 
                   total_staffed_bed = sum(NUM_STAFFE), 
                   total_icu_bed = sum(NUM_ICU_BE), 
                   median_bed_utiliz = median(BED_UTILIZ), 
                   potential_increase_beds = sum(Potential_)) %>%
  ungroup()

x.hospital.cori.hsa = x.hospital.cori %>% 
  st_drop_geometry() %>%
  group_by(geoid_hsa) %>% 
  dplyr::summarise(cori_hospital_count = n(hosid),
                   cori_hospital_name = str_c(hospital_name, collapse = ", "),
                   total_cori_hospital_beds = sum(beds)) %>%
  ungroup()



# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Combine hospitals w/ ACS
x.geo.county = st_read(coririsi_layer, 'geo_attr_county_pg')
x.health.attr.co = x.geo.county %>% 
  left_join(x.table.county.cori, by = c('geoid' = 'geoid')) %>% 
  left_join(x.hospital.beds.county, by = c('geoid' = 'geoid_co')) %>% 
  left_join(x.hospital.cori.county, by = c('geoid' = 'geoid_co')) 



x.geo.zcta = st_read(coririsi_layer, 'geo_attr_zcta510_pg')
x.health.attr.zc = x.geo.zcta %>% 
  left_join(x.table.zcta.cori, by = c('geoid' = 'geoid')) %>% 
  left_join(x.hospital.beds.zcta, by = c('geoid' = 'geoid_zc')) %>% 
  left_join(x.hospital.cori.zcta, by = c('geoid' = 'geoid_zc')) 


# HSA
x.health.attr.hsa = x.geo.zcta %>% 
  left_join(x.table.zcta.cori, by = c('geoid' = 'geoid')) %>% 
  left_join(x.src.hsa.zip.xw, by = c('geoid' = 'zipcode')) %>% 
  group_by(hsanum) %>% 
  dplyr::summarize(zip_count = n(zip),
                   hsacity = first(hsacity),
                   hsastate = first(hsastate),
                   total_population_2018 = sum(total_population_2018), 
                   population_19_over_2018 = sum(population_19_over_2018),
                   population_65_over_2018 = sum(population_65_over_2018),
                   population_85_over_2018 = sum(population_85_over_2018)) %>% 
  ungroup() %>% 
  left_join(x.hospital.beds.hsa, by = c('hsanum' = 'geoid_hsa'))


# HRR
x.health.attr.hrr = x.geo.zcta %>% 
  left_join(x.table.zcta.cori, by = c('geoid' = 'geoid')) %>% 
  left_join(x.src.hsa.zip.xw, by = c('geoid' = 'zipcode')) %>% 
  group_by(hrrnum) %>% 
  dplyr::summarize(zip_count = n(zip),
                   hrrcity = first(hrrcity),
                   hrrstate = first(hrrstate),
                   total_population_2018 = sum(total_population_2018), 
                   population_19_over_2018 = sum(population_19_over_2018),
                   population_65_over_2018 = sum(population_65_over_2018),
                   population_85_over_2018 = sum(population_85_over_2018)) %>% 
  ungroup() %>% 
  left_join(x.hospital.beds.hrr, by = c('hrrnum' = 'geoid_hrr'))

# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Write ----

write_layer(x.health.attr.zcta, "attr_zcta_health", db = T, ngacarto = T)
write_layer(x.health.attr.co, "attr_county_health", db = T, ngacarto = T)
write_layer(x.health.attr.hrr, "attr_hrr_health", db = T, ngacarto = T)
write_layer(x.health.attr.hsa, "attr_hsa_health", db = T, ngacarto = T)



















# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# OLD
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====

# HSA ----
x.geo.zcta = st_read(coririsi_layer, "geo_attr_zcta510_pg") %>%
  left_join(x.src.hsa.zip.xw, by = c('zip' = 'zipcode'))

# Read HSA/ZIP crosswalk
x.src.hsa.zip.xw = read_xls("./source/ZipHsaHrr17.xls") %>% 
  dplyr::mutate(zipcode = str_pad(zipcode2017, 5, "left", "0"))

# Write table
dbWriteTable(coririsi_source, "hrr_hsa_zipcode_crosswalk", x.src.hsa.zip.xw, overwrite = T)

x.table.zcta.cori.hsa = x.geo.zcta %>% 
  left_join(x.table.zcta.cori, by = c('zip' = 'geoid')) %>% 
  group_by(hsanum) %>% 
  dplyr::summarize(zip_count = n(zip),
                   total_population_2018 = sum(total_population_2018), 
                   population_65_over_2018 = sum(population_65_over_2018))


plot(x.table.zcta.cori.hsa$geom)


write_layer(x.table.zcta.cori.hsa, "acs_hsa", db = T, ngacarto = T)



x.hospitals = st_read(coririsi_layer, "hospitals_cori_pt") %>% 
  st_transform(4269) %>% 
  dplyr::filter(status == "OPEN",
                beds != -999)


x.table.zcta.cori.hsa.pt = x.table.zcta.cori.hsa %>% 
  st_join(x.hospitals) %>% 
  group_by(hsanum) %>% 
  dplyr::summarise(zip_count = first(zip_count), 
                   total_population_2018 = first(total_population_2018),
                   population_65_over_2018 = first(population_65_over_2018),
                   hospital_names = paste0(HOSPITAL_N, collapse = ", ", na.skip=T),
                   num_staff = sum(coalesce.na(NUM_STAFFE, 0)),
                   num_bed = sum(coalesce.na(NUM_ICU_BE, 0)), 
                   avg_bed_utiliz = mean(BED_UTILIZ, na.skip = T),
                   beds_per_1000_elderly = num_bed*1000/population_65_over_2018
                   )


write_layer(x.table.zcta.cori.hsa.pt, "acs_hsa", db = T, ngacarto = T, ngacarto.overwrite = T)


