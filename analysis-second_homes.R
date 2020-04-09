source("../base/global.R")

x.second.home = st_read("./output/second_homes_preiminary_v0_1.gpkg")
#write_layer(x.second.home, "second_homes_preiminary_v0_1", db = T)

x.covid = st_read(coririsi_layer, "covid_19_county_04_08_2020") %>% 
  st_drop_geometry() %>%
  dplyr::select(geoid, starts_with("confirmed")) %>% 
  dplyr::mutate(confirm_pct_grow_last_3 = (confirmed_04_07_2020 - confirmed_04_04_2020)/confirmed_04_04_2020 * 100, 
                confirm_pct_grow_last_5 = (confirmed_04_07_2020 - confirmed_04_02_2020)/confirmed_04_04_2020 * 100, 
                confirm_pct_grow_last_7 = (confirmed_04_07_2020 - confirmed_03_31_2020)/confirmed_04_04_2020 * 100, 
                confirm_pct_grow_last_10 = (confirmed_04_07_2020 - confirmed_03_28_2020)/confirmed_04_04_2020 * 100) 

x.rural = st_read(coririsi_layer, "attr_county_full") %>%
  dplyr::select(geoid, rural_area_pct_cbsa, rural_area_pct_cms) %>% 
  st_drop_geometry()

x.hu = st_read(coririsi_layer, "acs_county_health") %>% 
  dplyr::select(geoid, total_housing_unit_2018)

x.second.home.geo = x.second.home %>%   
  st_drop_geometry() %>% 
  left_join(x.covid, by = "geoid") %>% 
  left_join(x.rural, by = "geoid") %>% 
  left_join(x.hu, by = "geoid") %>% 
  dplyr::mutate(second_home_pct = second_home_2018/total_housing_unit_2018 * 100,
                confirme_per_100k = confirmed_04_07_2020 * 100000/total_population_2018) %>%
  dplyr::filter(second_home_pct > 10,
                confirmed_04_07_2020 > 0) %>% 
  replace(., is.na(.), 0)

x.second.home.geo.high = x.second.home.geo %>% 
  dplyr::filter(second_home_pct > 30,
                confirme_per_100k > 100) %>% 
  dplyr::select(geoid, namelsad, st_stusps, second_home_2018, second_home_pct, confirmed_04_07_2020, confirme_per_100k, rural_area_pct_cms)


names(x.second.home.geo.high)

normalize <- function(x) {
  return ((x - min(x)) / (max(x) - min(x)))
}
flattenCorrMatrix <- function(cormat, pmat) {
  ut <- upper.tri(cormat)
  data.frame(
    row = rownames(cormat)[row(cormat)[ut]],
    column = rownames(cormat)[col(cormat)[ut]],
    cor  =(cormat)[ut],
    p = pmat[ut]
  )
}
x.second.home.corr <- rcorr(as.matrix(x.second.home.geo %>% dplyr::select_if(is.numeric)))
x.second.home.corr.df = as.data.frame(x.second.home.corr$r) %>% rownames_to_column()
x.second.home.corr.df = flattenCorrMatrix(x.second.home.corr$r, x.second.home.corr$P)

res <- cor(x.second.home.geo %>% dplyr::select_if(is.numeric) %>% dplyr::select(-rural_area_pct_cbsa))
round(res, 2)

corrplot(res, type = "upper", order = "hclust", 
         tl.col = "black", tl.srt = 45, tl.cex = 0.8)

cor(x.second.home.geo$confirmed_04_07_2020, x.second.home.geo$second_home_2018)

plot(y = x.second.home.geo$confirmed_04_07_2020, x.second.home.geo$second_home_pct)
plot(y = x.second.home.geo$second_home_2018, x.second.home.geo$second_home_pct)
hist(x.second.home.geo$confirme_per_100k, breaks = 50, xlim = c(0, 1000))


x.second.home.lm = lm(x.second.home.geo$confirmed_04_07_2020 ~ x.second.home.geo$second_home_2018)
summary(x.second.home.lm)

