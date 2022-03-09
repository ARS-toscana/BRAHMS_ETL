
###########################################################
# insert name of your DAP (i.e. "ARS","DepLazio","Messina")
DAP<-"ARS"
###########################################################


# Call library:
if (!require("data.table")) install.packages("data.table")
library(data.table)
if (!require("haven")) install.packages("haven")
library(haven)
if (!require("lubridate")) install.packages("lubridate")
library(lubridate)
if (!require("stringr")) install.packages("stringr")
library(stringr)

# Define folders:
#dirbase<-c("\\\\nas.ars.toscana.it/public/FEPI/BRAHMS/")
dirbase<-getwd()
# diroutput<-paste0(thisdir,"/20220210_BRAHMS_CDM/")
diroutput<-paste0(dirbase,"CDMtables/")
dirinput<-paste0(thisdir,"/20210525_TheShinISS_CDM/")
# dirinput<-paste0(dirbase,"2202/")
# dirtemp<-paste0(thisdir,"/temp/")
dirtemp<-paste0(dirbase,"temp/")
dirmacro <- paste0(dirbase,"p_macro/")

# Check if those folders exist
if (file.exists(diroutput)){
  setwd(file.path(diroutput))
} else {
  suppressWarnings(dir.create(file.path( diroutput)))
  setwd(file.path(diroutput))
}

# if (file.exists(dirtemp)){
#   setwd(file.path(dirtemp))
# } else {
#   suppressWarnings(dir.create(file.path(dirtemp)))
#   setwd(file.path(dirtemp))
# }

gap_allowed_thisdatasource = 21
source(paste0(dirmacro,"CreateSpells_v15.R"))

setwd(thisdir)

# Define parameters:
date_format<-"%Y%m%d"
date_end<-"9999-12-31"
end_study<-as.Date("2023-12-31")
# upper_date<- as.Date(as.character(20160630), date_format)
# lower_date<- as.Date(as.character(20110601), date_format)
# lookback<-5*365.25
# fup<-3*365.25


##Define study parameters
derm_specility_sdo<-c("52") #inizia per 52 in repartodim
derm_specility_spa<-c("52") #reparto 52 in codbranca

psi_speciality_sdo<-c("40")
psi_speciality_spa<-c("40")
onco_speciality_sdo<-c("64")
onco_speciality_spa<-c("64")
surg_speciality_sdo<-c("06","07","09","10","11","12","13","14")
surg_speciality_spa<-c("06","07","09","10","11","12","13","14")
int_med_speciality_sdo<-c("26")
int_med_speciality_spa<-c("26")
  