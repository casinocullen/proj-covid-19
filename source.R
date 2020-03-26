

# hospitals
url_to_s3(url = "https://opendata.arcgis.com/datasets/1044bb19da8d4dbfb6a96eb1b4ebf629_0.zip", 
          filename = "1044bb19da8d4dbfb6a96eb1b4ebf629_0.zip", 
          s3path = "source/data-hospitals/", 
          s3bucket =  "cori-layers")


x.hospitals = st_read('./source/Definitive_Healthcare_USA_Hospital_Beds.shp')%>% 
  st_transform(4269)
names(x.hospitals)

dbWriteTable(coririsi_source, "definitive_healthcare_hospital_beds", x.hospitals)
