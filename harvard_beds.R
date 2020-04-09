
require(tidyverse)
require(sf)
source("../base/connections/coririsi_source.R")
source("../base/connections/coririsi_layer.R")
source("../base/functions/write_layer.R")

# Get Harvard data
score = dbReadTable(coririsi_source, "hrr_scorecard") %>%
  filter(scenario == "60%")

# Get zipcode HRR/HSA crosswalk
crosswalk = dbReadTable(coririsi_source, "hrr_hsa_zipcode_crosswalk") %>%
  mutate(zip = str_pad(zipcode2017, 5, "left", "0"))


# Get Zipcode polygons
acs.zcta = dbReadTable(coririsi_layer, "acs_zcta")
geo.zcta = st_read(coririsi_layer, "attr_zcta") %>%
  left_join(acs.zcta, by = c("geoid" = "GEOID"))

# Create HRR geo and add harvard data
hrr = geo.zcta %>%
  left_join(crosswalk, by = c("geoid" = "zip")) %>%
  group_by(hrrnum, hrrcity, hrrstate) %>%
  dplyr::summarize(
     pop = sum(total_population_2018),
     rural_area_pct_cms = mean(rural_area_pct_cms)
    ) %>%
  mutate(HRR = paste0(hrrcity, ", ", hrrstate))

hrr.score = hrr %>%
  left_join(score, by = c("HRR" = "HRR"))

write_layer(hrr.score, "harvard_score_3", fs = T, s3 = F, db = T, ngacarto = T, ngacarto.overwrite = T)




# Create HSA geo and add harvard data

  
  
# Add harvard data to zip





## EXAMPLE CODE

# Write table
dbWriteTable(coririsi_source, "hrr_hsa_zipcode_crosswalk", x.src.hsa.zip.xw, overwrite = T)



x.table.zcta.cori.hsa = x.geo.zcta %>% 
  left_join(x.table.zcta.cori, by = c('zip' = 'geoid')) %>% 
  group_by(hsanum) %>% 
  dplyr::summarize(zip_count = n(zip),
                   total_population_2018 = sum(total_population_2018), 
                   population_65_over_2018 = sum(population_65_over_2018))


plot(x.table.zcta.cori.hsa$geom)


write_layer(x.table.zcta.cori.hsa, "acs_hsa", db = T, ngacarto = T)



x.table.zcta.cori.hsa.pt = x.table.zcta.cori.hsa %>% 
  st_join(x.hospitals) %>% 
  group_by(hsanum) %>% 
  dplyr::summarise(zip_count = first(zip_count), 
                   total_population_2018 = first(total_population_2018),
                   population_65_over_2018 = first(population_65_over_2018),
                   hospital_names = paste0(HOSPITAL_N, collapse = ", ", na.skip=T),
                   num_staff = sum(coalesce.na(NUM_STAFFE, 0)),
                   num_bed = sum(coalesce.na(NUM_ICU_BE, 0)), 
                   avg_bed_utiliz = mean(BED_UTILIZ, na.skip = T),
                   beds_per_1000_elderly = num_bed*1000/population_65_over_2018
                   )


write_layer(x.table.zcta.cori.hsa.pt, "acs_hsa", db = T, ngacarto = T, ngacarto.overwrite = T)


