# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Number of staff  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))


source("../base/global.R")


url_to_s3(url = "http://download.cms.gov/nppes/NPPES_Data_Dissemination_March_2020.zip", 
          filename = "NPPES_Data_Dissemination_March_2020.zip", 
          s3path = "source/proj-covid-19/", 
          s3bucket =  "cori-layers")

unzip("./source/NPPES_Data_Dissemination_March_2020.zip", exdir = "./source/")


# Read layer
x.npi = st_read(coririsi_layer, "npi_taxonomy_group") 

x.npi[c('lon','lat')] = x.npi %>% st_coordinates()

x.npi = x.npi %>%
  st_drop_geometry()

x.npi.summary = GetCodebookFieldValues(x.npi %>% as.data.frame, x.max.value.cnt = 300)

x.npi.grp = x.npi %>% 
  group_by(taxonomycode, providertype, providersubtype, detailedspecialty) %>% 
  dplyr::summarise(count = n(npi))

ss = "https://docs.google.com/spreadsheets/d/1tMPN1A60ixs6brmOW7ccp194UnHJgsw3VWOasZHy9r8/edit#gid=0"
x.list.taxnomy = read_sheet(ss) %>% 
  dplyr::filter(`RG Use?` == "Y") %>%
  dplyr::select(taxonomycode)

names(x.list.taxnomy)

x.npi.filter = x.npi %>% 
  dplyr::filter(taxonomycode %in% x.list.taxnomy[['taxonomycode']]) %>% 
  st_as_sf(coords = c('lon', 'lat'), crs = 4269)
                  
                  
x.attr.county = st_read(coririsi_layer, 'geo_attr_county_pg') %>% 
  dplyr::select(geoid)

x.npi.filter.county =  x.attr.county %>%
  st_join(x.npi.filter) %>% 
  group_by(geoid) %>% 
  dplyr::summarise(total_staff = n(npi), 
                   providertype_aprn = sum(providertype=="APRNs and PAs", na.rm = TRUE), 
                   providertype_emergency = sum(providertype=="Emergency Medicine", na.rm = TRUE), 
                   providertype_nurses = sum(providertype=="Licensed and Registered Nurses", na.rm = TRUE), 
                   providertype_respiratory = sum(providertype=="Respiratory Specialists", na.rm = TRUE), 
                   providertype_specialty = sum(providertype=="Specialty Physician", na.rm = TRUE))

 

x.npi.filter.grp = x.npi.filter %>% 
  group_by(taxonomycode, providertype, providersubtype, detailedspecialty) %>% 
  dplyr::summarise(count = n(npi))

write_layer(x.npi.filter.county, "hospital_critical_staff_by_county", db = T, new.server = T, new.server.overwrite = T, ngacarto = T, ngacarto.overwrite = T)
                  
