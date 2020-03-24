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


names(x.table.county)

# ZCTA ----
## Table
x.table.zcta <- tidycencusTable("zcta")
x.table.zcta.cori <- tidycencusCORI(x.table.zcta) %>%
  dplyr::select(geoid = GEOID, ends_with("2018"))

x.table.zcta.cori = dbReadTable(coririsi_layer, "acs_zcta_health")

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

# hospitals
url_to_s3(url = "https://opendata.arcgis.com/datasets/1044bb19da8d4dbfb6a96eb1b4ebf629_0.zip", 
          filename = "1044bb19da8d4dbfb6a96eb1b4ebf629_0.zip", 
          s3path = "source/data-hospitals/", 
          s3bucket =  "cori-layers")


x.hospitals = st_read('./source/Definitive_Healthcare_USA_Hospital_Beds.shp')%>% 
  st_transform(4269)
names(x.hospitals)

dbWriteTable(coririsi_source, "definitive_healthcare_hospital_beds", x.hospitals)

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


