# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Number of staff per county ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----

source("../base/global.R")


# Reading all finished drivetime batches ----

# Save batch3
x.dt.batch3.raw = st_read("./output/x.npi.filter.dt.batch.3.gpkg")
write_layer(x.dt.batch3.raw, "npi_drivetime_40_75_b3", db = T)

# Save batch4
x.dt.batch4.raw.cc = st_read("./output/x.npi.filter.dt.batch.4.gpkg")
x.dt.batch4.raw.cr = st_read("./output/x.npi.filter.dt.batch.4.chris.gpkg")
x.dt.batch4.raw = rbind(x.dt.batch4.raw.cc, x.dt.batch4.raw.cr)
write_layer(x.dt.batch4.raw, "npi_drivetime_40_75_b4", db = T)

# Save batch5 = batch5+batch6+batch7
x.dt.batch5.raw = st_read("./output/x.npi.filter.dt.batch.5.gpkg")
x.dt.batch6.raw = st_read("./output/x.npi.filter.dt.batch.6.gpkg")
x.dt.batch7.raw = st_read("./output/x.npi.filter.dt.batch.7.gpkg")

x.dt.batch5.combined = rbind(x.dt.batch5.raw, x.dt.batch6.raw, x.dt.batch7.raw)
write_layer(x.dt.batch5.combined, "npi_drivetime_40_75_b5", db = T)

# Save batch8
x.dt.batch8.raw = st_read("./output/x.npi.filter.dt.batch.8.gpkg")
write_layer(x.dt.batch8.raw, "npi_drivetime_40_75_b8", db = T)

# Save to postgres
x.dt.batch.all = st_read(coririsi_layer, "npi_drivetime_40_75_b1", crs = 4269) %>% 
  dplyr::rename(geom = geometry) %>%
  rbind(st_read(coririsi_layer, "npi_drivetime_40_75_b2", crs = 4269) %>%
          dplyr::rename(geom = geometry)) %>%
  rbind(st_read(coririsi_layer, "npi_drivetime_40_75_b3", crs = 4269)) %>%
  rbind(st_read(coririsi_layer, "npi_drivetime_40_75_b4", crs = 4269)) %>%
  rbind(st_read(coririsi_layer, "npi_drivetime_40_75_b5", crs = 4269)) %>%
  rbind(st_read(coririsi_layer, "npi_drivetime_40_75_b8", crs = 4269)) %>% 
  distinct(npi, driveTime, .keep_all = T)


write_layer(x.dt.batch.all, "npi_drivetime_40_75", db = T)

# # Find points missing 75 mins and rerun
# # DONE
# x.dt.batch.all.missing.75 = x.dt.batch.all %>% 
#   st_drop_geometry() %>%
#   dplyr::group_by(npi) %>% 
#   dplyr::summarise(has_40 = sum(driveTime=="40", na.rm = TRUE), 
#                    has_75 = sum(driveTime=="75", na.rm = TRUE)) %>% 
#   dplyr::filter(has_75 == 0)
# 
# x.dt.batch.all.missing.75.full = x.npi %>% 
#   dplyr::filter(npi %in% x.dt.batch.all.missing.75$npi)
# 
# saveRDS(x.dt.batch.all.missing.75.full, "./output/x.npi.filter.batch.8.RDS")

# Read only 40 mins
x.dt.batch.all.40 = st_read(coririsi_layer, "npi_drivetime_40_75", crs = 4269) %>% 
  dplyr::filter(driveTime == 40) 

# Read only 75 mins
x.dt.batch.all.75 = st_read(coririsi_layer, "npi_drivetime_40_75", crs = 4269) %>% 
  dplyr::filter(driveTime == 75) 

names(x.dt.batch.all.75)

# Agg. to county
x.geo.county = st_read(coririsi_layer, 'geo_attr_county_pg') %>% 
  dplyr::select(geoid)

x.staff.drivetime.county.pg.40 = x.geo.county %>% 
  st_join(x.dt.batch.all.40) %>% 
  dplyr::group_by(geoid) %>% 
  dplyr::summarise(total_staff_dt = n_distinct(npi), 
                   providertype_aprn_dt = sum(providertype=="APRNs and PAs", na.rm = TRUE), 
                   providertype_emergency_dt = sum(providertype=="Emergency Medicine", na.rm = TRUE), 
                   providertype_nurses_dt = sum(providertype=="Licensed and Registered Nurses", na.rm = TRUE), 
                   providertype_respiratory_dt = sum(providertype=="Respiratory Specialists", na.rm = TRUE), 
                   providertype_specialty_dt = sum(providertype=="Specialty Physician", na.rm = TRUE))

x.staff.drivetime.county.pg.75 = x.geo.county %>% 
  st_join(x.dt.batch.all.75) %>% 
  dplyr::group_by(geoid) %>% 
  dplyr::summarise(total_staff_dt_75 = n_distinct(npi), 
                   providertype_aprn_dt_75 = sum(providertype=="APRNs and PAs", na.rm = TRUE), 
                   providertype_emergency_dt_75 = sum(providertype=="Emergency Medicine", na.rm = TRUE), 
                   providertype_nurses_dt_75 = sum(providertype=="Licensed and Registered Nurses", na.rm = TRUE), 
                   providertype_respiratory_dt_75 = sum(providertype=="Respiratory Specialists", na.rm = TRUE), 
                   providertype_specialty_dt_75 = sum(providertype=="Specialty Physician", na.rm = TRUE))

# Write layer
write_layer(x.staff.drivetime.county.pg.40, "npi_drivetime_40_county", db = T)
write_layer(x.staff.drivetime.county.pg.75, "npi_drivetime_75_county", db = T)

# Previous staff by points
x.staff.drivetime.county.pg.40 = st_read(coririsi_layer, "npi_drivetime_40_county")%>% 
  st_drop_geometry() 
x.staff.drivetime.county.pg.75 = st_read(coririsi_layer, "npi_drivetime_75_county")%>% 
  st_drop_geometry() 

x.hospital.drivetime.county.pg = st_read(coririsi_layer, "county_preparedness_score") %>% 
  dplyr::select(geoid, name, st_stusps, total_estimated_bed_40_mins)%>% 
  st_drop_geometry() 

x.staff.county.pt = st_read(coririsi_layer, "hospital_critical_staff_by_county")%>% 
  st_drop_geometry() 

x.rural = st_read(coririsi_layer, "attr_county_full") %>%
  dplyr::select(geoid, rural_area_pct_cbsa, rural_area_pct_cms) %>% 
  st_drop_geometry()

x.staff.county.compare = x.staff.drivetime.county.pg.40 %>%
  left_join(x.rural , by = 'geoid') %>%
  left_join(x.staff.drivetime.county.pg.75 , by = 'geoid') %>%
  left_join(x.staff.county.pt, by = 'geoid') %>% 
  left_join(x.hospital.drivetime.county.pg, by = 'geoid') %>% 
  dplyr::select(geoid, name, st_stusps, rural_area_pct_cbsa, rural_area_pct_cms,starts_with("total_staff"), total_estimated_bed_40_mins) %>% 
  dplyr::rename(total_staff_dt_40 = total_staff_dt)

names(x.staff.county.compare)

write.csv(x.staff.county.compare, "./output/raw_bed_staff_40_min.csv", row.names = F)

ss = "https://docs.google.com/spreadsheets/d/1W1M0j4NF6GQgsxPG9pNaspT-unlhqfEW6pTnffDzGcQ/edit#gid=233626807"
sheets_write(ss, data = x.staff.county.compare, sheet = "raw data compare")


