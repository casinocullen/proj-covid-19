# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Number of staff per county ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----

source("../base/global.R")

x.state = st_read(coririsi_layer, "geo_attr_state_pg") %>%
  st_drop_geometry() %>% 
  dplyr::select(stusps, region)
  
x.state.geo = st_read(coririsi_layer, "geo_attr_state_pg") %>%
  dplyr::select(stusps, region)

x.def.health = st_read(coririsi_layer, "definitive_healthcare_hospital_beds") %>% 
  st_drop_geometry() %>% 
  dplyr::group_by(HQ_STATE) %>% 
  dplyr::summarise(icu_state = sum(NUM_ICU_BE)) %>% 
  dplyr::mutate(st_full = state2abb(HQ_STATE, convert = T)) %>% 
  dplyr::arrange(st_full) %>% 
  data.frame()

ss = "https://docs.google.com/spreadsheets/d/1blKlsHm2YKPdE_MKkp1bM-DNWpdFXMi61PGb76QFhQw/edit#gid=0"
sheets_write(ss, data = x.def.health, sheet = "raw")

x.compare = read_sheet(ss, sheet = "x.def.health") %>% 
  left_join(x.state, by = c('HQ_STATE' = "stusps")) 
  
# x.compare.geo = read_sheet(ss, sheet = "x.def.health") %>%
#   left_join(x.state.geo, by = c('HQ_STATE' = "stusps"))

x.model = lm(vent_state ~ icu_state + factor(region), data = x.compare, na.action = na.exclude)
x.model = lm(vent_state ~ icu_state, data = x.compare.geo, na.action = na.exclude)
summary(x.model)

predict.lm(x.model, x.compare[, c('icu_state', 'region')], interval = "prediction")
x.compare.pred = cbind(x.compare, vent_pred = predict.lm(x.model, x.compare[, c('icu_state', 'region')],  interval = "prediction"))

x.compare.pred = x.compare.pred %>% 
  dplyr::mutate(vent_final = ifelse(is.na(vent_state), vent_pred.fit, vent_state))

write_layer(x.compare.pred, "vent_lm", ngacarto = T, ngacarto.overwrite = T)

# # Compare all features
# x.attr.state = st_read(coririsi_layer, "attr_county_health")
# 
# x.compare.all = x.compare.pred %>% 
#   left_join(x.attr.state, by = c('HQ_STATE' = "stusps")) %>% 
#   dplyr::select_if(is.numeric)
# 
# 
# x.corr = data.frame(cor(x.compare.all[-3], x.compare.all$resid, use = 'na.or.complete'))


# County Interfer
x.def.health.county = st_read(coririsi_layer, "definitive_healthcare_hospital_beds") %>% 
  st_drop_geometry() %>% 
  dplyr::group_by(geoid_co, HQ_STATE) %>% 
  dplyr::summarise(icu_county = sum(NUM_ICU_BE)) %>% 
  ungroup()

x.def.health.county.state = x.def.health.county %>% 
  dplyr::left_join(x.compare.pred, by = c('HQ_STATE')) %>%
  dplyr::mutate(icu_share = icu_county/icu_state,
                vent_county = round(vent_final*icu_share),
                vent_county_high = round(vent_pred.upr*icu_share),
                vent_county_low = round(vent_pred.lwr*icu_share),
                vent_state_high = round(vent_pred.upr),
                vent_state_low = round(vent_pred.lwr)) %>% 
  dplyr::select(geoid_co, state = HQ_STATE, vent_county, vent_county_high, vent_county_low, vent_state = vent_final, vent_state_high, vent_state_low)

write.csv(x.def.health.county.state, file = "ventilator_state_county_est", row.names = F)


# county geo
x.geo.county = st_read(coririsi_layer, "geo_attr_county_pg") %>% 
  dplyr::select(geoid)

x.compare.county.pred.geo = x.geo.county %>% 
  dplyr::left_join(x.compare.county.pred, by = c('geoid' = 'geoid_co')) %>% 
  dplyr::filter(HQ_STATE == 'ND') %>% 
  dplyr::mutate(vent_county = round(vent_county))


ggplot(data = x.compare.county.pred.geo) +
  geom_sf() +
  geom_sf(data = x.compare.county.pred.geo, fill = NA) + 
  geom_sf_text(aes(label = vent_county), size = 3)


# County share
x.county.pred = data.frame(vent_county_pred = predict.lm(x.model, x.def.health.county[, c('icu_state', 'region')]))

x.def.health.county.state = x.def.health.county %>% 
  dplyr::rename(icu_county = icu_state) %>%
  left_join(x.compare, by = c("HQ_STATE", "st_full", "region")) %>% 
  dplyr::mutate(icu_share = icu_county/icu_state,
                vent_county = vent_state*icu_share)

x.compare.county.pred.geo = x.geo.county %>% 
  dplyr::left_join(x.def.health.county.state, by = c('geoid' = 'geoid_co')) %>% 
  dplyr::filter(HQ_STATE == 'ND') %>% 
  dplyr::mutate(vent_county = round(vent_county))




ggplot(data = x.compare.county.pred.geo) +
  geom_sf() +
  geom_sf(data = x.compare.county.pred.geo, fill = NA) + 
  geom_sf_text(aes(label = vent_county), size = 3)
