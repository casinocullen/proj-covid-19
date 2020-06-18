# Overview
This is the data analyzing repository for COVID-19 and its impacts on the county level.
Data sources:
* Johns Hopkins University (https://github.com/CSSEGISandData/COVID-19)
* Institute for Health Metrics and Evaluation (IHME) (https://covid19.healthdata.org/united-states-of-america)
* Definitive healthcare (https://www.definitivehc.com/)
* CMS NPPES NPI Registry (https://npiregistry.cms.hhs.gov/)
* CDC Social Vulnerability Index (SVI) (https://svi.cdc.gov/index.html)

# Code descriptions
- auto_update_county_preparedness.R
    * Generate the county prepardness score (published in https://www.statnews.com/feature/coronavirus/county-preparedness-scores/)
- SIR - Clean.ipynb
    * Testing Susceptible-Infected-Removed (SIR) model at the county level
- covid_19_daily_update.R
    * Automate daily COVID case/recovery/death data ETL
- geo_county_rollup.sql
    * SQL query that rollup block level data to the county level
- hospital_staff_drivetime_definitive.R
    * Calculate drive time from point data (location of critical hospital staff)

## Note
Some of scripts require the data connections from the database. Please contact with author for more details.