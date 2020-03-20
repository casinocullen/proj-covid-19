### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
###   NAME: 
###   DESC: 
### AUTHOR: 
### OUTPUT: 
###  -
###  INPUT: 
###  -
###  NOTES:
### <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>


# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# SETUP  ----

setwd(dirname(rstudioapi::getActiveDocumentContext()$path)); getwd();

# FUNCTIONS
source("../base/functions/utilityFunctions.R")
 #source("../base/")

# PACKAGES
 #require("")



# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# NEW CODE   ----

















# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# SNIPPETS   ----

# SEARCH PostgreSQL  ----
#x.src.list = searchDB(sterm="^attr_[^_].*state", x.db=coririsi_layer, exclude="") ; x.src.list

# read PostgreSQL Table  ----
  #x.src.0 = dbGetQuery(coririsi_layer, paste0("SELECT * FROM ", x.src.list[1]))
  #saveRDS2("x.src.0")


# WRITE codebooks ----
  #x.src.cb = writeSourceCodebook(x.db = coririsi_source,  x.sql.table.nm="power_plants_20190801", x.codebook.nm="TEMP Data Codebook - TEST", x.schema.nm="sch_source")
  #x.src.cb = writeLayerCodebook(x.db = coririsi_layer, x.sql.table.nm="attr_state",               x.codebook.nm="TEMP Layer Codebook - TEST", x.schema.nm="sch_layer")

# WRITE layer ----
  #write_layer(sf=x.src.0, layer.name=paste0("TEMP"), layer.type="pg", db=F, fs=T, s3=F, ngacarto=T)







# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# EOF 
# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
