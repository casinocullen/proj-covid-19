# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# HRR Scorecard ----
# ≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠≠====
# Database connections ----
rm(list=ls(pattern=("(^x\\.)")))


source("../base/global.R")

URL = "https://docs.google.com/spreadsheets/u/1/d/1xAyBFTrlxSsTKQS7IDyr_Ah4JLBYj6_HX6ijKdm4fAY/htmlview?sle=true#gid=0"

hrr_score_20 = googlesheets4::read_sheet(URL, sheet = "20% Population") %>% 
  dplyr::mutate(scenario = "20%") %>% 
  dplyr::filter(!is.na(`Total Hospital Beds`))

hrr_score_40 = googlesheets4::read_sheet(URL, sheet = "40% Population")%>% 
  dplyr::mutate(scenario = "40%") %>% 
  dplyr::filter(!is.na(`Total Hospital Beds`))

hrr_score_60 = googlesheets4::read_sheet(URL, sheet = "60% Population")%>% 
  dplyr::mutate(scenario = "60%") %>% 
  dplyr::filter(!is.na(`Total Hospital Beds`))


hrr_score_all = rbind(hrr_score_20, hrr_score_40, hrr_score_60)

colnames(hrr_score_all) = gsub(pattern = "\\Eighteen Months\\b", "18_mon", colnames(hrr_score_all))
colnames(hrr_score_all) = gsub(pattern = "\\Twelve Months\\b", "12_mon", colnames(hrr_score_all))
colnames(hrr_score_all) = gsub(pattern = "\\Six Months\\b", "6_mon", colnames(hrr_score_all))

dbWriteTable(coririsi_source, "hrr_scorecard", hrr_score_all, overwrite = T)
