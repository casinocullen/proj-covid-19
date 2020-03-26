### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
###   Layer: COVID-19 by county
###   Source: JHU (https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_daily_reports/03-24-2020.csv)
### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>


# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# SETUP  ----

setwd(dirname(rstudioapi::getActiveDocumentContext()$path)); getwd();

# FUNCTIONS
source("../base/global.R")
x.date <- as.character(Sys.Date(), format = '%m_%d_%Y')

st <- as.Date("2020-3-22")
en <- Sys.Date()
all.dates <- as.character(seq(en, st, by = "-1 day"), format = '%m-%d-%Y')

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
                    !!paste0("active_", date):= Active) %>%
      dplyr::select(geoid, name, st_stusps, starts_with("confirmed_"), starts_with("deaths_"), starts_with("recovered_"), starts_with("active_")) %>% 
      janitor::clean_names()
  }
}

# write
write_layer(x.geo.county.covid, paste0("covid_19_county_", x.date), db = T)
write_layer(x.geo.county.covid, "covid_19_county_latest", db = T)
