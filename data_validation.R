

# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Data Validation  ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
all_index = st_read(coririsi_layer, "county_preparedness_score") %>% 
  dplyr::mutate(prep_score = (0.5*bed_score_1 + 0.5*staff_score_1 + dem_score_1 + se_score_1 + proj_score_1) / 4,
                prep_score_old = (bed_score_1 + staff_score_1 + dem_score_1 + se_score_1 + proj_score_1) / 5,
                prep_score_1 = ntile(prep_score, 100),
                prep_score_old = ntile(prep_score_old, 100),
                prep_level = case_when(prep_score_1 <= 20 ~ "Very Low", 
                                       prep_score_1 > 20 & prep_score_1 <= 40 ~ "Low", 
                                       prep_score_1 > 40 & prep_score_1 <= 60 ~ "Medium", 
                                       prep_score_1 > 60 & prep_score_1 <= 80 ~ "High", 
                                       prep_score_1 > 80 ~ "Very High"))

write_layer_absolute(all_index, layer.name = "county_preparedness_score_v0_4", layer.type="pg", db=T, fs=T, new.server = T, new.server.overwrite = T)

all_index_lm = lm(data = all_index, formula = all_index$prep_score_1 ~ all_index$total_estimated_bed_40_mins_100k + all_index$total_estimated_bed_40_mins_100k + all_index$total_staff_dt_100k + all_index$svi_socioeconomic + all_index$icuover_max_needed_100k)

summary(all_index_lm)

plot(all_index_lm)
anova(all_index_lm)

library("Hmisc")
all_index_num = all_index %>% dplyr::select_if(is.numeric) %>% st_drop_geometry
all_index_vars = all_index[,c("total_estimated_bed_40_mins_100k",
                              "total_staff_dt_100k",
                              "pct_65_over_2018",
                              "svi_socioeconomic",
                              "icuover_max_needed_100k")]%>% st_drop_geometry

all_index_corr <- rcorr(as.matrix(all_index_num))

# Functions
flattenCorrMatrix <- function(cormat, pmat) {
  ut <- upper.tri(cormat)
  data.frame(
    row = rownames(cormat)[row(cormat)[ut]],
    column = rownames(cormat)[col(cormat)[ut]],
    cor  =(cormat)[ut],
    p = pmat[ut]
  )
}
rsq <- function (x, y) cor(x, y) ^ 2

# Correlation/r^2 for 
all_index_corr_df = flattenCorrMatrix(all_index_corr$r, all_index_corr$P)
all_index_corr = as.data.frame(all_index_corr$r) %>% rownames_to_column()
all_index_r2_df =  as.data.frame(cor(all_index_num)^2) %>% rownames_to_column()


# Write googlesheet
ss = "https://docs.google.com/spreadsheets/d/1W1M0j4NF6GQgsxPG9pNaspT-unlhqfEW6pTnffDzGcQ/edit#gid=0"
googlesheets4::sheets_write(ss, data = all_index %>% st_drop_geometry())
googlesheets4::sheets_write(ss, data = all_index_corr,sheet = "correlation")
googlesheets4::sheets_write(ss, data = all_index_corr_df,sheet = "correlation long")
googlesheets4::sheets_write(ss, data = all_index_r2_df,sheet = "r-sq")

# Plot
library(corrplot)
res <- cor(all_index_vars)
round(res, 2)

corrplot(res, type = "upper", order = "hclust", 
         tl.col = "black", tl.srt = 45, tl.cex = 0.8)

# Histogram
hist.data.frame(all_index_vars)

library(PerformanceAnalytics)
chart.Correlation(all_index_vars, histogram=TRUE, pch=19)

