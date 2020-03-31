### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
###   Layer: COVID-19 by county
###   Source: JHU (https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_daily_reports/03-24-2020.csv)
### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>


# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# SETUP  ----
# FUNCTIONS
#source("/data/Github/base/global.R")
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

x.geo.county = st_read(coririsi_layer, "geo_attr_county_pg")


x.geo.county.covid = x.geo.county
for (date in all.dates) {
  x.covid.daily = NULL
  URL = paste0("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/", date, ".csv")
  
  tryCatch(expr = {x.covid.daily = read.csv(URL, stringsAsFactors = F, colClasses = c("FIPS"="character"))}, 
           error = function(e) e)
  
  if (!is.null(x.covid.daily)) {
    x.geo.county.covid = x.geo.county.covid %>% 
      left_join(x.covid.daily, by = c('geoid' = "FIPS")) %>% 
      dplyr::mutate(!!paste0("confirmed_", date) := Confirmed, 
                    !!paste0("deaths_", date):= Deaths, 
                    !!paste0("recovered_", date):= Recovered, 
                    !!paste0("active_", date):= Active,
                    latest_update = date) %>%
      dplyr::select(geoid, name, st_stusps, latest_update, starts_with("confirmed_"), starts_with("deaths_"), starts_with("recovered_"), starts_with("active_")) %>% 
      janitor::clean_names()
    
    latest_update = gsub("-", "_", date)
  }
}

x.geo.county.covid.latest = x.geo.county.covid %>% 
  dplyr::select(geoid, name, st_stusps, latest_update, ends_with(latest_update)) %>% 
  dplyr::rename(confirm := !!paste0("confirmed_", latest_update), 
                deaths := !!paste0("deaths_", latest_update), 
                recovered := !!paste0("recovered_", latest_update), 
                active := !!paste0("active_", latest_update))

# write
write_layer_absolute(x.geo.county.covid, paste0("covid_19_county_", x.date), db = T, fs = T)
write_layer_absolute(x.geo.county.covid.latest, "covid_19_county_latest", db = T, fs = T, ngacarto = T, ngacarto.overwrite = T, new.server = T, new.server.overwrite = T)
