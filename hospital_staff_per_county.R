source("../base/global.R")



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



# Read only 40 mins
x.dt.batch1 = st_read(coririsi_layer, "npi_drivetime_40_75_b1", crs = 4269) %>% 
  dplyr::filter(driveTime == 40) %>%
  dplyr::rename(geom = geometry)
names(x.dt.batch1)

x.dt.batch2 = st_read(coririsi_layer, "npi_drivetime_40_75_b2", crs = 4269) %>% 
  dplyr::filter(driveTime == 40)%>%
  dplyr::rename(geom = geometry)
names(x.dt.batch2)

x.dt.batch3 = st_read(coririsi_layer, "npi_drivetime_40_75_b3", crs = 4269) %>%
  dplyr::filter(driveTime == 40)
names(x.dt.batch3)

x.dt.batch4 = st_read(coririsi_layer, "npi_drivetime_40_75_b4", crs = 4269) %>%
  dplyr::filter(driveTime == 40)
names(x.dt.batch4)

x.dt.batch5 = st_read(coririsi_layer, "npi_drivetime_40_75_b5", crs = 4269) %>%
  dplyr::filter(driveTime == 40)
names(x.dt.batch5)

x.dt.batch.all = rbind(x.dt.batch1, x.dt.batch2, x.dt.batch3, x.dt.batch4, x.dt.batch5)

x.summary = GetCodebookFieldValues(x.df = x.dt.batch.all %>% st_drop_geometry, x.max.value.cnt = 100)

# Agg. to county
x.geo.county = st_read(coririsi_layer, 'geo_attr_county_pg') %>% 
  dplyr::select(geoid)

x.staff.drivetime.county.pg = x.geo.county %>% 
  st_join(x.dt.batch.all) %>% 
  dplyr::group_by(geoid) %>% 
  dplyr::summarise(total_staff_dt = n_distinct(npi), 
                   providertype_aprn_dt = sum(providertype=="APRNs and PAs", na.rm = TRUE), 
                   providertype_emergency_dt = sum(providertype=="Emergency Medicine", na.rm = TRUE), 
                   providertype_nurses_dt = sum(providertype=="Licensed and Registered Nurses", na.rm = TRUE), 
                   providertype_respiratory_dt = sum(providertype=="Respiratory Specialists", na.rm = TRUE), 
                   providertype_specialty_dt = sum(providertype=="Specialty Physician", na.rm = TRUE))

# Write layer
write_layer(x.staff.drivetime.county.pg, "npi_drivetime_40_county", db = T)

# Previous staff by points
x.staff.county.pt = st_read(coririsi_layer, "hospital_critical_staff_by_county")

x.staff.county.compare = x.staff.drivetime.county.pg %>% 
  st_drop_geometry() %>%
  left_join(x.staff.county.pt %>% st_drop_geometry(), by = 'geoid')

ss = "https://docs.google.com/spreadsheets/d/1W1M0j4NF6GQgsxPG9pNaspT-unlhqfEW6pTnffDzGcQ/edit#gid=233626807"


