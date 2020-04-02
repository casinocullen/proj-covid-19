source("../base/global.R")

x.layer.prep = st_read(coririsi_layer, "county_preparedness_score") %>% st_drop_geometry()
names(x.layer.prep)

library(treemap)

p <- treemap(x.layer.prep,
             # data
             index="st_stusps",
             vSize="prep_score_1",
             type="index",
             #aspRatio = 2,
             
             # Main
             title="",
             palette="Dark2",
             
             # Borders:
             border.col=c("black", "grey", "grey"),             
             border.lwds=c(1,0.5,0.1),                         
             
             # Labels
             fontsize.labels=c(0.7, 0.4, 0.3),
             fontcolor.labels=c("white", "white", "black"),
             fontface.labels=1,            
             bg.labels=c("transparent"),              
             align.labels=list( c("center", "center"), c("left", "top"), c("right", "bottom")),                                  
             overlap.labels=0.5,
             inflate.labels=T   
             
)

png(filename="tree.png",width=8000, height=5000)
dev.off()


