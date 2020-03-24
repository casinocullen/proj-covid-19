### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
###   Layer: Hospitals beds by county
###   Source: Hospitals drive-time
### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>


# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# SETUP  ----

setwd(dirname(rstudioapi::getActiveDocumentContext()$path)); getwd();

# FUNCTIONS
source("../base/global.R")

# ---------------------------------------------------------------------------
# Step 1: 
# Layer Table ----
# ---------------------------------------------------------------------------
x.hospital.dt = st_read(coririsi_layer, "hospitals_drivetime_cori_pg") %>%
  st_transform(crs = 4269) %>% 
  dplyr::filter(type %in% c('CRITICAL ACCESS', 'GENERAL ACUTE CARE'), 
                status == "OPEN") %>% 
  dplyr::select(-geom, -geoid) %>% 
  dplyr::mutate(beds = ifelse(beds == -999, 0, beds),
                hospital_name = name)

x.hospital.dt.dis = x.hospital.dt %>% 
  group_by(county) %>%
  dplyr::summarize(beds = sum(beds),
                   hospitals_name_40_mins = str_c(hospital_name, collapse = ", ")) %>% 
  ungroup()

plot(x.hospital.dt.dis$geometry)


x.hospital = st_read(coririsi_source, "definitive_healthcare_hospital_beds") %>% 
  dplyr::filter(HOSPITAL_T %in% c('Critical Access Hospital', 'Short Term Acute Care Hospital', 'Long Term Acute Care Hospital', 'VA Hospital')) 



x.geo.county = st_read(coririsi_layer, 'geo_attr_county_pg') %>% 
  dplyr::select(geoid, geoid_st, name, namelsad)

# Join by point
x.geo.county.join.pt = x.geo.county %>% 
  st_join(x.hospital) %>% 
  dplyr::group_by(geoid) %>% 
  dplyr::summarise(hospitals_name = str_c(HOSPITAL_N, collapse = ", "),
                   county_name = first(name),
                   total_licensed_bed = sum(NUM_LICENS), 
                   total_staffed_bed = sum(NUM_STAFFE), 
                   total_icu_bed = sum(NUM_ICU_BE), 
                   median_bed_utiliz = median(BED_UTILIZ), 
                   potential_increase_beds = sum(Potential_)) 

# Join by drive time
x.geo.county.join.pg = x.geo.county %>% 
  st_join(x.hospital.dt) %>% 
  dplyr::group_by(geoid) %>% 
  dplyr::summarise(hospitals_name_40_mins = str_c(hospital_name, collapse = ", "),
                   total_estimated_bed_40_mins = sum(beds)) %>%
  st_drop_geometry()

# ACS county
x.acs.county = dbReadTable(coririsi_layer, "acs_county_health")



# Broadband
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
  dplyr::select(geoid = geoid_co,
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



x.geo.county.join.all = x.geo.county.join.pt %>% 
  left_join(x.geo.county.join.pg, by = "geoid") %>% 
  left_join(x.acs.county, by = "geoid") %>% 
  left_join(x.la.county, by = 'geoid')
 
write_layer(x.geo.county.join.all, "telehealth_county", db = T, ngacarto = T, ngacarto.overwrite = T, new.server = T, new.server.overwrite = T)

names(x.geo.county.join.all)



