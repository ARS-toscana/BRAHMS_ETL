# Call library:
if (!require("data.table")) install.packages("data.table")
library(data.table)
if (!require("haven")) install.packages("haven")
library(haven)
if (!require("lubridate")) install.packages("lubridate")
library(lubridate)

# Define folders:
diroutput<-paste0(thisdir,"/20210518_output/")
dirinput<-paste0(thisdir,"/input_corrected/")
dirtemp<-paste0(thisdir,"/temp/")

# Check if those folders exist
if (file.exists(diroutput)){
  setwd(file.path(diroutput))
} else {
  suppressWarnings(dir.create(file.path( diroutput)))
  setwd(file.path(diroutput))
}

if (file.exists(dirtemp)){
  setwd(file.path(dirtemp))
} else {
  suppressWarnings(dir.create(file.path(dirtemp)))
  setwd(file.path(dirtemp))
}

setwd(thisdir)

# Define parameters:
date_format<-"%Y%m%d"
# upper_date<- as.Date(as.character(20160630), date_format)
# lower_date<- as.Date(as.character(20110601), date_format)
# lookback<-5*365.25
# fup<-3*365.25

# List all datasets:
spa<-paste0("SPA",rep(2007:2019))
# num<-c()
# for (i in rep(01:12)) {
#   if(nchar(i)==1) {
#     rep<-paste0(0,i)
#     num<-c(num,rep)
#   }
# }
# num<-c(num, 10:12)
# spa_m<-paste0(spa,rep(num,each=13))

spf<-paste0("SPF",rep(2007:2019)) 
cap<-paste0("CAP",rep(1:2))
#fed<-paste0("FED",rep(2007:2019))

alldatasets<-c("ID","ANA","EXE","FED","SDO", spf)
