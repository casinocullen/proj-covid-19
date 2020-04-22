source("../base/global.R")


# hospitals
url_to_s3(url = "https://opendata.arcgis.com/datasets/1044bb19da8d4dbfb6a96eb1b4ebf629_0.zip", 
          filename = "1044bb19da8d4dbfb6a96eb1b4ebf629_0.zip", 
          s3path = "source/data-hospitals/", 
          s3bucket =  "cori-layers")

unzip("./source/1044bb19da8d4dbfb6a96eb1b4ebf629_0.zip", exdir = "./source/")


x.hospitals = st_read('./source/Definitive_Healthcare_USA_Hospital_Beds.shp')%>% 
  st_transform(4269) %>% 
  dplyr::mutate_if(is.factor, as.character) %>% 
  dplyr::mutate(PEDI_ICU_B = as.numeric(PEDI_ICU_B),
                BED_UTILIZ = as.numeric(BED_UTILIZ))

x.hospitals[c('longitude', 'latitude')] = st_coordinates(x.hospitals)

names(x.hospitals)

dbWriteTable(coririsi_source, "definitive_healthcare_hospital_beds", x.hospitals, overwrite = T)


# Compare with old version
x.hospitals.old = st_read(coririsi_source, "definitive_healthcare_hospital_beds") %>% 
  dplyr::mutate(version = "OLD") %>% 
  dplyr::select("HOSPITAL_N", "HQ_ADDRESS", version) %>% 
  st_drop_geometry()

nrow(unique(x.hospitals[c("HOSPITAL_N", "HQ_ADDRESS")] %>% st_drop_geometry()))

x.hospitals.combine = x.hospitals %>%
  dplyr::mutate(version = "NEW") %>% 
  full_join(x.hospitals.old, by = c("HOSPITAL_N", "HQ_ADDRESS"))

