# FUNCTIONS
source("../base/global.R")



x.la.county = dbReadTable(coririsi_analysis, "la_counties_broadband") %>% 
  dplyr::select(geoid_co, hh2018_sum, hu2018_sum, pop2018_sum, bl_count, rural_cms_blcnt,                        
                rural_eda_blcnt,
                rural_lisc_blcnt,
                rural_fhfa_blcnt,
                rural_forhp_blcnt,
                rural_far_blcnt, 
                rural_cbsa_blcnt,
                rural_msa_blcnt, 
                rural_uc_blcnt,  
                rural_rucc_blcnt,
                rural_uic_blcnt, 
                rural_ruca_blcnt,
                rural_usda_blcnt,
                rural_ur10_blcnt,
                starts_with("f477_")) %>% 
  dplyr::mutate(geoid_co,
                pop2018_sum,
                rural_eda_blpct = round(rural_eda_blcnt/bl_count * 1000)/10, 
                rural_uc_blpct = round(rural_uc_blcnt/bl_count * 1000)/10, 
                rural_usda_blpct = round(rural_usda_blcnt/bl_count * 1000)/10, 
                rural_ruca_blpct = round(rural_ruca_blcnt/bl_count * 1000)/10, 
                rural_uic_blpct = round(rural_uic_blcnt/bl_count * 1000)/10, 
                rural_cbsa_blpct = round(rural_cbsa_blcnt/bl_count * 1000)/10, 
                rural_cms_blpct = round(rural_cms_blcnt/bl_count * 1000)/10, 
                f477_maxadup_less_3_poppct = round((1 - coalesce(f477_maxadup_2018dec_3_popsum, 0)/pop2018_sum) * 1000)/10, 
                f477_maxaddown_less_25_poppct = round((1 - coalesce(f477_maxaddown_2018dec_25_popsum, 0)/pop2018_sum) * 1000)/10, 
                f477_maxad_downup_2018dec_less_25_3_poppct = round((1 - coalesce(f477_maxad_downup_2018dec_25_3_popsum, 0)/pop2018_sum) * 1000)/10,
                f477_maxad_downup_2018dec_less_25_3_blpct = round((1 - coalesce(f477_maxad_downup_2018dec_25_3_blcnt, 0)/bl_count) * 1000)/10, 
                f477_maxaddown_2018dec_avg_pop_wt = round(f477_maxaddown_2018dec_pop_sum/pop2018_sum), 
                f477_maxadup_2018dec_avg_pop_wt = round(f477_maxadup_2018dec_pop_sum/pop2018_sum),
                f477_maxadup_2018dec_range = paste0(f477_maxadup_2018dec_min, " - ", f477_maxadup_2018dec_max), 
                f477_maxaddown_2018dec_range = paste0(f477_maxaddown_2018dec_min, " - ", f477_maxaddown_2018dec_max)) %>% 
  dplyr::select(geoid_co,
                pop2018_sum,
                hh2018_sum, 
                hu2018_sum,
                rural_eda_blpct,
                rural_uc_blpct,
                rural_usda_blpct,
                rural_ruca_blpct,
                rural_uic_blpct,
                rural_cbsa_blpct,
                rural_cms_blpct,
                f477_maxaddown_2018dec_min, 
                f477_maxadup_2018dec_min,
                f477_maxaddown_2018dec_max, 
                f477_maxadup_2018dec_max,
                f477_maxadup_2018dec_range, 
                f477_maxaddown_2018dec_range,
                f477_maxadup_less_3_poppct,
                f477_maxaddown_less_25_poppct,
                f477_maxad_downup_2018dec_less_25_3_poppct,
                f477_maxad_downup_2018dec_less_25_3_blpct,
                f477_maxaddown_2018dec_avg_pop_wt,
                f477_maxadup_2018dec_avg_pop_wt)

# Add county geo layer
x.geo.county = st_read(coririsi_layer, "geo_attr_county_pg")

x.la.county.geo = x.geo.county %>% 
  left_join(x.la.county, by = c('geoid' = 'geoid_co')) %>%
  st_make_valid()


write_layer(x.la.county.geo, "broadband_access_county", ngacarto = T, db = T)
