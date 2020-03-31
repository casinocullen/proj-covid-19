### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
###   Layer: COVID-19 Time series
###   Source: Hospitals drive-time
### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>


# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# SETUP  ----

# FUNCTIONS
require(tidyverse)
require(sf)
require(DBI)
source("/data/Github/base/functions/write_layer_absolute.R")


# cori-risi connection
#   host: cori-risi
#     DB: data
# Schema: layer

coririsiconf <- config::get("cori-risi", file = "/data/Github/base/config.yml")
coririsi_layer = dbConnect(
  RPostgres::Postgres(),
  user     = coririsiconf$user,
  password = coririsiconf$password,
  dbname   = coririsiconf$dbname,
  host     = coririsiconf$host,
  port     = coririsiconf$port,
  options  =  '-c search_path=sch_layer,public'
)
#dbListTables(coririsi)

rm(coririsiconf)



x.date <- as.character(Sys.Date(), format = '%m_%d_%Y')

st <- as.Date("2020-3-22")
en <- Sys.Date()
all.dates <- as.character(seq(st, en, by = "+1 day"), format = '%m-%d-%Y')

x.geo.county.covid = NULL
for (date in all.dates) {
  #x.covid.daily = NULL
  URL = paste0("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/", date, ".csv")
  
  tryCatch(expr = {x.covid.daily = read.csv(URL, stringsAsFactors = F, colClasses = c("FIPS"="character"))}, 
           error = function(e) e)
  
  if (!is.null(x.covid.daily)) {
    x.covid.daily = x.covid.daily %>% 
      dplyr::mutate(latest_update = date)
    
    x.geo.county.covid = rbind(x.geo.county.covid, x.covid.daily) 
    
  } 
}

x.geo.county = st_read(coririsi_layer, 'geo_attr_county_pg') %>% 
  st_centroid()
x.geo.county[, c('lon', 'lat')] = st_coordinates(x.geo.county)


x.geo.county.covid.long = x.geo.county %>% 
  st_drop_geometry() %>% 
  st_as_sf(coords = c('lon', 'lat'), crs = 4269) %>% 
  inner_join(x.geo.county.covid, by = c('geoid' = "FIPS"))  %>% 
  dplyr::mutate(latest_update = as.Date(latest_update, format = '%m-%d-%Y'))
  


write_layer_absolute(x.geo.county.covid.long, layer.name = "covid_19_county_long", db = T, new.server = T, new.server.overwrite = T)

