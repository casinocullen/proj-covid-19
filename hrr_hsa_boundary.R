# HRR + HSA boundary

source("../base/global.R")


url_to_s3(url = "https://atlasdata.dartmouth.edu/downloads/geography/hsa_bdry.zip", filename = "hsa_bdry.zip", 
          s3path = "source/data-hospitals/", 
          s3bucket =  "cori-layers")


url_to_s3(url = "https://atlasdata.dartmouth.edu/downloads/geography/hrr_bdry.zip", filename = "hrr_bdry.zip", 
          s3path = "source/data-hospitals/", 
          s3bucket =  "cori-layers")

url_to_s3(url = "https://atlasdata.dartmouth.edu/DATA/load_files/2010_2014_census_data/ctsfe_vt.zip", 
          filename = "ctsfe_vt.zip", 
          s3path = "source/data-hospitals/", 
          s3bucket =  "cori-layers")

url_to_s3(url = "https://atlasdata.dartmouth.edu/downloads/geography/ZipHsaHrr17.xls", 
          filename = "ZipHsaHrr17.xls", 
          s3path = "source/data-hospitals/", 
          s3bucket =  "cori-layers")


unzip("./source/ctsfe_vt.zip", exdir = "./source/")

unzip("./source/hrr_bdry.zip", exdir = "./source/")
unzip("./source/hsa_bdry.zip", exdir = "./source/")

x.src.hrr = st_read("./source/hrr_bdry.gpkg")
x.src.hrr.bed = fread("./source/Hospital_Referral_Region.csv") %>% 
  dplyr::select(-the_geom)
x.src.hrr.total = x.src.hrr %>% 
  left_join(x.src.hrr.bed, by = c("hrrnum" = "hrr_num"))

x.src.hsa.zip.xw = read_xls("./source/ZipHsaHrr17.xls") %>% 
  dplyr::mutate(zipcode = str_pad(zipcode2017, 5, "left", "0"))

x.geo.zcta = st_read(coririsi_layer, "geo_attr_zcta510_pg") %>%
  left_join(x.src.hsa.zip.xw, by = c('zip' = 'zipcode'))

x.geo.zcta.hsa = x.geo.zcta %>% 
  group_by(hsanum) %>% 
  dplyr::summarize(zip_count = n(zip))



x.src.hsa = st_read("./source/hsa_bdry.gpkg")

x.src.hsa.total = x.src.hsa %>% 
  dplyr::left_join()


