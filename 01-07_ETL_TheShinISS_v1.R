#################################################################################################
## ETL  ARS -> TheShinISS ####

# Version 1.0
# 21-05-2021
# Author: CB 

#################################################################################################

rm(list=ls(all.names=TRUE))

# set the directory where the file is saved as the working directory
thisdir<-dirname(rstudioapi::getSourceEditorContext()$path)
setwd(thisdir)

#load parameters
source(paste0(thisdir,"/parameters_toTheShinISS.R"))
setwd(thisdir)

# Read and check data -----------------------------------------------------

# for (source in alldatasets){
#   pippo <- as.data.table(read_dta(paste0(thisdir,"/input_corrected/",source,".dta")))
#   assign(source,pippo)
#   setkeyv(pippo,"IDUNI")
# }
# rm(pippo)

# For each row of table 3.1 implement the corrisponding specification table



# 1.	Use SDO to populate RICOVERI_OSPEDALIERI  --------

SDO <- as.data.table(read_dta(paste0(dirinput,"/SDO.dta"))) 
setkeyv(SDO,"IDUNI") 


# transform and create variable:
RICOVERI_OSPEDALIERI<-SDO

# rename variables:
setnames(RICOVERI_OSPEDALIERI, old = "IDUNI", new = "id")
setnames(RICOVERI_OSPEDALIERI, old = "DATAMM", new = "data_a")
setnames(RICOVERI_OSPEDALIERI, old = "DATDIM", new = "data_d")
setnames(RICOVERI_OSPEDALIERI, old = "DIADIM", new = "codcmp")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI", new = "intproc") 
setnames(RICOVERI_OSPEDALIERI, old = "PAT1", new = "codcm1")
setnames(RICOVERI_OSPEDALIERI, old = "PAT2", new = "codcm2")
setnames(RICOVERI_OSPEDALIERI, old = "PAT3", new = "codcm3")
setnames(RICOVERI_OSPEDALIERI, old = "PAT4", new = "codcm4")
setnames(RICOVERI_OSPEDALIERI, old = "PAT5", new = "codcm5")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI2", new = "intsec1")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI3", new = "intsec2")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI4", new = "intsec3")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI5", new = "intsec4")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI6", new = "intsec5")
setnames(RICOVERI_OSPEDALIERI, old = "REGIME", new = "regric")
setnames(RICOVERI_OSPEDALIERI, old = "REPAMM", new = "repartoam")
setnames(RICOVERI_OSPEDALIERI, old = "REPDIM", new = "repartodim")
# setnames(RICOVERI_OSPEDALIERI, old = "REP1", new = "repartotras1")
# setnames(RICOVERI_OSPEDALIERI, old = "REP2", new = "repartotras2")
# setnames(RICOVERI_OSPEDALIERI, old = "REP3", new = "repartotras3")
setnames(RICOVERI_OSPEDALIERI, old = "MODIM", new = "tipdim")
setnames(RICOVERI_OSPEDALIERI, old = "DRG", new = "drg")
#setnames(RICOVERI_OSPEDALIERI, old = "codosp", new = "cod_struttura")

# chenge modality to tipdim
RICOVERI_OSPEDALIERI<-RICOVERI_OSPEDALIERI[tipdim=="2" |tipdim=="3" |tipdim=="4" | tipdim=="5" | tipdim=="7" | tipdim=="A" | tipdim=="10" | tipdim=="11",tipdim:=0][tipdim=="1",tipdim:=2][tipdim=="6" |tipdim=="8" |tipdim=="9" ,tipdim:=1][tipdim=="99" |tipdim=="999" ,tipdim:=3]

# keep only which needed
RICOVERI_OSPEDALIERI <- RICOVERI_OSPEDALIERI[,.(id, codcmp, codcm1, codcm2, codcm3, codcm4, codcm5, intproc, intsec1, intsec2, intsec3, intsec4, intsec5, data_a, data_d, regric, repartoam,repartodim, drg, tipdim)] # repartotras1, repartotras2, repartotras3, , cod_struttura

fwrite(RICOVERI_OSPEDALIERI, paste0(diroutput,"RICOVERI_OSPEDALIERI_SDO.csv"), quote = "auto")


rm(SDO)
rm(RICOVERI_OSPEDALIERI)


# 1a. Use SDOTEMP to populate RICOVERI_OSPEDALIERI --------

SDOTEMP <- as.data.table(read_dta(paste0(dirinput,"/SDOTEMP.dta")))
setkeyv(SDOTEMP,"IDUNI")

# transform and create variable:
RICOVERI_OSPEDALIERI<-SDOTEMP

# rename variables:
setnames(RICOVERI_OSPEDALIERI, old = "IDUNI", new = "id")
setnames(RICOVERI_OSPEDALIERI, old = "DATAMM", new = "data_a")
setnames(RICOVERI_OSPEDALIERI, old = "DATDIM", new = "data_d")
setnames(RICOVERI_OSPEDALIERI, old = "DIADIM", new = "codcmp")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI", new = "intproc") 
setnames(RICOVERI_OSPEDALIERI, old = "PAT1", new = "codcm1")
setnames(RICOVERI_OSPEDALIERI, old = "PAT2", new = "codcm2")
setnames(RICOVERI_OSPEDALIERI, old = "PAT3", new = "codcm3")
setnames(RICOVERI_OSPEDALIERI, old = "PAT4", new = "codcm4")
setnames(RICOVERI_OSPEDALIERI, old = "PAT5", new = "codcm5")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI2", new = "intsec1")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI3", new = "intsec2")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI4", new = "intsec3")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI5", new = "intsec4")
setnames(RICOVERI_OSPEDALIERI, old = "CODCHI6", new = "intsec5")
setnames(RICOVERI_OSPEDALIERI, old = "REGIME", new = "regric")
setnames(RICOVERI_OSPEDALIERI, old = "REPAMM", new = "repartoam")
setnames(RICOVERI_OSPEDALIERI, old = "REPDIM", new = "repartodim")
# setnames(RICOVERI_OSPEDALIERI, old = "REP1", new = "repartotras1")
# setnames(RICOVERI_OSPEDALIERI, old = "REP2", new = "repartotras2")
# setnames(RICOVERI_OSPEDALIERI, old = "REP3", new = "repartotras3")
setnames(RICOVERI_OSPEDALIERI, old = "MODIM", new = "tipdim")
setnames(RICOVERI_OSPEDALIERI, old = "DRG", new = "drg")
#setnames(RICOVERI_OSPEDALIERI, old = "codosp", new = "cod_struttura")


# keep only which needed
RICOVERI_OSPEDALIERI <- RICOVERI_OSPEDALIERI[,.(id, codcmp, codcm1, codcm2, codcm3, codcm4, codcm5, intproc, intsec1, intsec2, intsec3, intsec4, intsec5, data_a, data_d, regric, repartoam,repartodim, drg, tipdim)] # repartotras1, repartotras2, repartotras3, , cod_struttura

fwrite(RICOVERI_OSPEDALIERI, paste0(diroutput,"RICOVERI_OSPEDALIERI_SDOTEMP.csv"), quote = "auto")


rm(SDOTEMP)
rm(RICOVERI_OSPEDALIERI_SDOTEMP)

# 2.  Use SPA  to populate SPECIALISTICA --------------

for (source in spa){
  pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
  assign(source,pippo)
  setkeyv(pippo,"IDUNI")
  
  pluto<-get(source)

  setnames(pluto, old="IDUNI",new="id")
  setnames(pluto, old="DATAINI",new="dataprest")
  setnames(pluto, old="SPECIALI",new="codbranca")
  setnames(pluto, old="CODPRES",new="codprest")

  #patesen	Esenzione per patologia
  
  
  pippo<-pippo[,.(id,codprest,dataprest,codbranca)] #, patesen
  

  assign(paste0("SPECIALISTICA_",substr(source,4,7)),pippo)
  setkeyv(pippo,"id")
  
  fwrite(pippo, paste0(diroutput,"/",paste0("SPECIALISTICA_SPA_",substr(source,4,7)),".csv"), quote = "auto")
  
}
rm(pippo, pluto)


rm(list=ls(pattern="^SPA"))
rm(list=ls(pattern="^SPECIALISTICA"))

# 3.  Use PS  to populate PRONTO_SOCCORSO ------------------------------------------

PS <- as.data.table(read_dta(paste0(dirinput,"/PS.dta")))
setkeyv(PS,"IDUNI")


PRONTO_SOCCORSO<-PS

# rename variables:
setnames(PRONTO_SOCCORSO, old = "IDUNI", new = "id")
setnames(PRONTO_SOCCORSO, old = "DATA_ORA_ACCETTAZ", new = "data_a")
setnames(PRONTO_SOCCORSO, old = "DATA_ORA_DIMI", new = "data_d")
setnames(PRONTO_SOCCORSO, old = "COD_DIAGNOSI_ARSNEW", new = "codcmp") #DIAGNOSI_PRINC
# setnames(PRONTO_SOCCORSO, old = "DIAGNOSI_PRINC", new = "codcmp")
# setnames(PRONTO_SOCCORSO, old = "DIAGNOSI_PRINC", new = "codcmp")
# setnames(PRONTO_SOCCORSO, old = "DIAGNOSI_PRINC", new = "codcmp")
# setnames(PRONTO_SOCCORSO, old = "DIAGNOSI_PRINC", new = "codcmp")
setnames(PRONTO_SOCCORSO, old = "ESITO", new = "tipdim")

# keep only needed variables
PRONTO_SOCCORSO<-PRONTO_SOCCORSO[,.(id, data_a, data_d, codcmp, tipdim)] #codcm1, codcm2, codcm3, codcm4,

fwrite(PRONTO_SOCCORSO, paste0(diroutput,"/PRONTO_SOCCORSO_PS.csv"), quote = "auto")

rm(PS)

# 4.  Use EXE to populate ESENZIONI ------------------------------------------

EXE <- as.data.table(read_dta(paste0(dirinput,"/EXE.dta")))
setkeyv(EXE,"IDUNI")


ESENZIONI<-EXE

# rename variables:
setnames(ESENZIONI, old = "IDUNI", new = "id")
setnames(ESENZIONI, old = "RILASCIO", new = "datai")
setnames(ESENZIONI, old = "SCADENZA", new = "dataf")
setnames(ESENZIONI, old = "ICD9CM", new = "ese_cod")

# keep only needed variables
ESENZIONI<-ESENZIONI[,.(id, ese_cod, datai, dataf)]

fwrite(ESENZIONI, paste0(diroutput,"/ESENZIONI_EXE.csv"), quote = "auto")

rm(EXE)

# NO -5.  Use SALM to populate EVENTS and MEDICAL_OBSERVATIONS  # solo EVENTS  ---------------
# 
# SALM <- as.data.table(read_dta(paste0(dirinput,"/SALM.dta")))
# setkeyv(SALM,"IDUNI")
# 
# 
# ##Specification table EVENTS -SALM:
#   #- SALM: For each record
#     # Create one record of EVENTS for each non-empty value of DIAGNOSI_PRINCIPALE, COMORBIDITA_1, COMORBIDITA_2
#     # Copy the values of SALM into EVENTS according to the following table
# 
# EVENTS_SALM_<-SALM[DIAGNOSI_PRINCIPALE!=""|COMORBIDITA_1!="" | COMORBIDITA_2!="",][,visit_occurrence_id:=paste0("SALM_",seq_along(DATA))] 
# EVENTS_SALM_<-EVENTS_SALM_[SETTORE=="1",event_record_vocabulary:="ICD9"]
# EVENTS_SALM_<-EVENTS_SALM_[SETTORE=="2",event_record_vocabulary:="ICD10"]
# EVENTS_SALM_<-EVENTS_SALM_[,.(IDUNI,DATA,DIAGNOSI_PRINCIPALE, COMORBIDITA_1,COMORBIDITA_2,event_record_vocabulary,visit_occurrence_id)]
# 
# EVENTS_SALM<-melt(EVENTS_SALM_, measure= c("DIAGNOSI_PRINCIPALE","COMORBIDITA_1","COMORBIDITA_2"), variable.name = "ord", value.name = "event_code")[event_code!="",]
# 
# EVENTS_SALM<-EVENTS_SALM[,`:=`(end_date_record="",text_linked_to_event_code="",event_free_text="",present_on_admission="yes",origin_of_event="SALM", laterality_of_event="")]
# EVENTS_SALM<-EVENTS_SALM[ord=="DIAGNOSI_PRINCIPALE",meaning_of_event:="access_to_mental_health_service_primary"]
# EVENTS_SALM<-EVENTS_SALM[ord!="DIAGNOSI_PRINCIPALE",meaning_of_event:="access_to_mental_health_service_comorbidity"]
# 
# #30/09 modification: drop 0 or 00 before ICD10 code
# EVENTS_SALM<-EVENTS_SALM[event_record_vocabulary=="ICD10" & nchar(event_code)>2, event_code:=gsub("^00","",event_code)]
# EVENTS_SALM<-EVENTS_SALM[event_record_vocabulary=="ICD10" & nchar(event_code)>1, event_code:=gsub("^0","",event_code)]
#   
# # rename variables:
# setnames(EVENTS_SALM, old = "IDUNI", new = "person_id")
# setnames(EVENTS_SALM, old = "DATA", new = "start_date_record")
# 
# EVENTS_SALM<-EVENTS_SALM[,.(person_id,start_date_record,end_date_record,event_code, event_record_vocabulary,text_linked_to_event_code,event_free_text,present_on_admission,laterality_of_event,meaning_of_event,origin_of_event,visit_occurrence_id)]
# 
# EVENTS_SALM<-EVENTS_SALM[,`:=`(start_date_record=format(start_date_record, "%Y%m%d"))]
# 
# fwrite(EVENTS_SALM, paste0(diroutput,"/EVENTS_SALM.csv"), quote = "auto")
# rm(EVENTS_SALM_)
# 
# ## Specification table MEDICAL_OBSERVATIONS -SALM:
#   #- SALM:
# 
# rm(SALM)

# 6.  Use FED to populate PRESCRIZIONI_FARMACI  --------------------------------------------

for (source in fed){
  pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
  assign(source,pippo)
  setkeyv(pippo,"IDUNI")
  
  setnames(pippo, old="IDUNI",new="id")
  setnames(pippo, old="DATAERO",new="datasped")
  setnames(pippo, old="CODFARM",new="aic")
  setnames(pippo, old="PEZZI_ARSNEW",new="pezzi")
  setnames(pippo, old="COD_ATC5",new="atc")
  

  pippo<-pippo[,.(id,datasped,aic,atc,pezzi)] 
  
  
  assign(paste0("PRESCRIZIONI_FARMACI_",substr(source,4,7)),pippo)
  setkeyv(pippo,"id")
  
  fwrite(pippo, paste0(diroutput,"/",paste0("PRESCRIZIONI_FARMACI_FED_",substr(source,4,7)),".csv"), quote = "auto")
  
}
rm(pippo)


rm(list=ls(pattern="^FED"))
rm(list=ls(pattern="^PRESCRIZIONI_FARMACI"))


# 6a. Use FEDTEMP to populate PRESCRIZIONI_FARMACI ---------------------------

# for (source in fedtemp){
#   pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
#   assign(source,pippo)
#   setkeyv(pippo,"IDUNI")
#   
#   setnames(pippo, old="IDUNI",new="id")
#   setnames(pippo, old="DATAERO",new="dataprest")
#   setnames(pippo, old="CODFARM",new="aic")
#   setnames(pippo, old="PEZZI_ARSNEW",new="pezzi")
#   setnames(pippo, old="COD_ATC5",new="atc")

#   
#   
#   pippo<-pippo[,.(id,dataprest,aic,atc,pezzi)] 
#   
#   
#   assign(paste0("PRESCRIZIONI_FARMACI_",substr(source,4,7)),pippo)
#   setkeyv(pippo,"id")
#   
#   fwrite(pippo, paste0(diroutput,"/",paste0("PRESCRIZIONI_FARMACI_FEDTEMP_",substr(source,4,7)),".csv"), quote = "auto")
#   
# }
# rm(pippo)
# 
# 
# #rm(list=ls(pattern="^FED"))
# rm(list=ls(pattern="^PRESCRIZIONI_FARMACI"))
# rm(FEDTEMP)

# 7.  Use SPF to populate PRESCRIZIONI_FARMACI --------------------------------------------

for (source in spf){
  pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
  assign(source,pippo)
  setkeyv(pippo,"IDUNI")
  
  setnames(pippo, old="IDUNI",new="id")
  setnames(pippo, old="DATAERO",new="datasped")
  setnames(pippo, old="CODFARM",new="aic")
  setnames(pippo, old="NUMFARM",new="pezzi")
  setnames(pippo, old="COD_ATC5",new="atc")
  
  
  pippo<-pippo[,.(id,datasped,aic,atc,pezzi)] 
  
  
  assign(paste0("PRESCRIZIONI_FARMACI_",substr(source,4,7)),pippo)
  setkeyv(pippo,"id")
  
  fwrite(pippo, paste0(diroutput,"/",paste0("PRESCRIZIONI_FARMACI_SPF_",substr(source,4,7)),".csv"), quote = "auto")
  
}
rm(pippo)


rm(list=ls(pattern="^PRESCRIZIONI_FARMACI"))
rm(list=ls(pattern="^SPF"))


# NO - 8.  Use CAP to populate SURVEY_ID, then SURVEY_OBSERVATIONS -------------
# 
# ## Specification table SURVEY_ID - CAP1: 
# # The local tables feeding this CDM table are: CAP, ABS, IVG, RMR. The rules to create SURVEY_ID from them are as follows
# 
# #- CAP: For each record
#   # ●	Create one record of SURVEY_ID for the mother, and as many records as the number of children and number the records with a sequential number stored in survey_id
#   # ●	Copy the values of CAP into SURVEY_ID according to the following table for each maternal record seq(1,uniqueN(IDUNI))
# for (source in cap){
#   pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
#   assign(source,pippo)
#   setkeyv(pippo,"IDUNI")
# }
# rm(pippo)
# 
# ## 12/04:added to remove empty IDUNI
# CAP1<-CAP1[IDUNI!="",]
# CAP2<-CAP2[IDUNI!="",]
# CAP1<-CAP1[!is.na(DATAPARTO_ARSNEW),]
# CAP2<-CAP2[DATAPARTO_ARSNEW!="",]
# 
# CAP1<-CAP1[,DATAPARTO_ARSNEW:=as.Date(DATAPARTO_ARSNEW)]
# CAP2<-CAP2[,DATAPARTO_ARSNEW:=as.Date(DATAPARTO_ARSNEW)]
# 
# # #12/04: verify unique pregnancy-row
# # CAP01<-unique(CAP1, by=c("IDUNI","DATAPARTO_ARSNEW","ID_CAP1_ARSNEW"))
# # CAP02<-unique(CAP2, by=c("IDUNI","DATAPARTO_ARSNEW","ID_CAP1_ARSNEW"))
# 
# # 14/04: change survey_id per twins
# CAP1<-CAP1[,id:=.GRP, by = IDUNI]
# setDT(CAP1)[,survey_id :=paste0("CAP_",id)][,-"id"]
# # 14/04: add unique
# CAP01<-unique(CAP1[,.(IDUNI,survey_id,DATAPARTO_ARSNEW)])
# SURVEY_ID<-CAP01[,`:=`(survey_meaning="birth_registry_mother",
#                        survey_origin="CAP1")]
# setnames(SURVEY_ID, old="IDUNI", new = "person_id")
# setnames(SURVEY_ID, old="DATAPARTO_ARSNEW", new = "survey_date")
# #rm(CAP10)
# 
# ## Specification table SURVEY_ID - CAP2: 
# # The local tables feeding this CDM table are: CAP, ABS, IVG, RMR. The rules to create SURVEY_ID from them are as follows
# 
# #- CAP: For each record
# # ●	Create one record of SURVEY_ID for the mother, and as many records as the number of children and number the records with a sequential number stored in survey_id
# # ●	Copy the values of CAP into SURVEY_ID according to the following table for each maternal record seq(1,uniqueN(IDUNI))
# 
# CAP20<-merge(CAP2,CAP1[,.(ID_CAP1_ARSNEW,survey_id)],by="ID_CAP1_ARSNEW")
# #14/04: add unique
# CAP202<-unique(CAP20[,.(IDUNI,survey_id,DATAPARTO_ARSNEW)])
# SURVEY_ID2<-CAP202[,`:=`(survey_meaning="birth_registry_mother",
#                          survey_origin="CAP2")]
# setnames(SURVEY_ID2, old="IDUNI", new = "person_id")
# setnames(SURVEY_ID2, old="DATAPARTO_ARSNEW", new = "survey_date")
# #rm(CAP20)
# 
# SURVEY_ID<-SURVEY_ID[,`:=`(survey_date=format(survey_date, "%Y%m%d"))]
# SURVEY_ID2<-SURVEY_ID2[,`:=`(survey_date=format(survey_date, "%Y%m%d"))]
# 
# # 21/04: verify unique person_survey
# SURV<-rbind(SURVEY_ID[,.(person_id,survey_id)],SURVEY_ID2[,.(person_id,survey_id)])
# dup_idx <- duplicated(SURV)
# SURV_rows <- SURV[dup_idx, survey_id]
# 
# SURVEY_ID <- SURVEY_ID[ ! SURVEY_ID$survey_id %in% SURV_rows, ]
# SURVEY_ID2 <- SURVEY_ID2[ ! SURVEY_ID2$survey_id %in% SURV_rows, ]
# 
# CAP1 <- CAP1[ ! CAP1$survey_id %in% SURV_rows, ]
# CAP2<- CAP2[ ! CAP2$survey_id %in% SURV_rows, ]
# 
# fwrite(SURVEY_ID, paste0(diroutput,"SURVEY_ID_CAP1.csv"), quote = "auto")
# fwrite(SURVEY_ID2, paste0(diroutput,"SURVEY_ID_CAP2.csv"), quote = "auto")
# 
# ## Specification table SURVEY_OBSERVATIONS - CAP1: 
# #The local tables feeding this CDM table are: CAP, ABS, IVG, RMR
# 
#   #-CAP:For each record of the mother
#     # ●	Extract from SURVEY_ID (above) the corresponding value of survey_id
#     # ●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below; 
# 
# #SURVEY_OBSERVATIONS_0<-CAP1[,survey_id:=seq_along(IDUNI)][,.(IDUNI,DATAPARTO_ARSNEW, survey_id)]
# #names(CAP1)
# CAP1<-CAP1[,DATAPARTO_ARSNEW:=as.character(DATAPARTO_ARSNEW)]
# 
# vars<-c("ABORTI","ACCR_FET","ALTEZZA","AMNIO","ANON_M","CESAREO_ARSNEW","CITTU_ARSNEW_M","CITTU_ARSNEW_P","CONCEP","CONDPR_M","CONDPR_P","CONSANG","DATNAS_P","DEC_GRAV","DOVE_GRAV","DOWN","ECO_22","ETA_M_ARSNEW","ETA_P_ARSNEW","FETOSCOP","FUMO","GENERE","GEST_ECO","IVG","METODO","NATIMORTI","NATIVIVI","NRECOGR","NRECOGR","NRFEM","NRIND","NRMASCHI","PARTI_ARSNEW","PESO_PRE","POSPR_M","POSPR_P","PRIMA_VI","RAMATT_M","RAMATT_P","RH","RIPRASS","SETTAMEN_ARSNEW","STATCIV_M","STATONAS_M_ARSNEW","STATONAS_P_ARSNEW","TITSTU_M_ARSNEW","TITSTU_P_ARSNEW","VILLI_C","VISITE_ARSNEW","DATAPARTO_ARSNEW") #"DATAPARTO_ARSNEW" added
# CAP10<-CAP1[ABORTI!="" | ACCR_FET!="" | ALTEZZA!="" | AMNIO!="" | ANON_M!="" | CESAREO_ARSNEW!="" | CITTU_ARSNEW_M!="" | CITTU_ARSNEW_P!="" | CONCEP!="" | CONDPR_M!="" | CONDPR_P!="" | CONSANG!="" | !is.na(DATNAS_P) | DEC_GRAV!="" | DOVE_GRAV!="" | DOWN!="" | ECO_22!="" | ETA_M_ARSNEW!="" | ETA_P_ARSNEW!="" | FETOSCOP!="" | FUMO!="" | GENERE!="" | GEST_ECO!="" | IVG!="" | METODO!="" | NATIMORTI!="" | NATIVIVI!="" | NRECOGR!="" | NRECOGR!="" | NRFEM!="" | NRIND!="" | NRMASCHI!="" | PARTI_ARSNEW!="" | PESO_PRE!="" | POSPR_M!="" | POSPR_P!="" | PRIMA_VI!="" | RAMATT_M!="" | RAMATT_P!="" | RH!="" | RIPRASS!="" | SETTAMEN_ARSNEW!="" | STATCIV_M!="" | STATONAS_M_ARSNEW!="" | STATONAS_P_ARSNEW!="" | TITSTU_M_ARSNEW!="" | TITSTU_P_ARSNEW!="" | VILLI_C!="" | VISITE_ARSNEW!=""| !is.na(DATAPARTO_ARSNEW),]
# 
# #14/04: add unique
# CAP101<-unique(CAP10,by=c("IDUNI","survey_id","DATAPARTO_ARSNEW"))
# 
# CAP11<-melt(CAP101, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value") 
# #SURVEY_OBSERVATIONS_CAP1<-CAP11[,survey_id:=paste0("SO_CAP1_",seq_along(IDUNI))]
# SURVEY_OBSERVATIONS_CAP1<-CAP11[,`:=`(so_source_table="CAP1",so_origin="CAP1", so_meaning="birth_registry_mother")]
# SURVEY_OBSERVATIONS_CAP1<-SURVEY_OBSERVATIONS_CAP1[so_source_column=="PESO_PRE",so_unit:="kg"]
# 
# 
# ## duplicate DATAPARTO_ARSNEW, as it is and in "so_date"
# SURVEY_OBSERVATIONS_CAP1<-merge(SURVEY_OBSERVATIONS_CAP1,CAP1[,.(IDUNI,ID_CAP1_ARSNEW,DATAPARTO_ARSNEW)],by=c("IDUNI","ID_CAP1_ARSNEW"),all.x = T)
# 
# setnames(SURVEY_OBSERVATIONS_CAP1, old="IDUNI", new="person_id")
# setnames(SURVEY_OBSERVATIONS_CAP1, old="DATAPARTO_ARSNEW", new="so_date")
# SURVEY_OBSERVATIONS_CAP1[,`:=`(so_date=as.Date(so_date))]
# SURVEY_OBSERVATIONS_CAP1[,`:=`(so_date=format(so_date, "%Y%m%d"))]
# SURVEY_OBSERVATIONS_CAP1[,.(person_id,so_date,so_source_table,so_source_column, so_source_value,so_unit,so_origin,so_meaning,survey_id)]
# 
# fwrite(SURVEY_OBSERVATIONS_CAP1, paste0(diroutput,"/SURVEY_OBSERVATIONS_CAP1.csv"), quote = "auto")
# rm(CAP10, CAP11, CAP01)
# 
# ## Specification table SURVEY_OBSERVATIONS - CAP2: 
# #The local tables feeding this CDM table are: CAP, ABS, IVG, RMR
# 
#   #-CAP2: For each record of each child
#     # ●	Extract from SURVEY_ID (above) the corresponding value of survey_id
#     # ●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below
# 
# CAP20<-CAP20[,DATAPARTO_ARSNEW:=as.character(DATAPARTO_ARSNEW)]
# 
# vars<-c("ALLATTA","ALTROSAN","ANESTES","APGAR","CARIOTIPO","CIRCOST","CIRC_CRA","DESMAL_1","DESMAL_2","D_CICOST","D_MAL_1M","D_MAL_2M","D_MAL_GR1","D_MAL_GR2","ES_STRUM","ES_STRUM","ETA_G_MAL","ETA_NEO","FARM_TRAV","FOTOGRAF","GENITALI","GINECOL","INDIFARM_ARSNEW","INDOTTO_ARSNEW","INTUBAZ","KRISTELLER","LUNGH","LUOGO","MALFOR","MALF_1","MALF_2","MALF_3","MALF_FRA","MALF_G_M","MALF_G_P","MALF_M","MALF_P","MALF_P_M","MALF_P_P","MAL_1","MAL_1_M","MAL_2","MAL_2_M","MAL_GR_1","MAL_GR_2","MOD_PART","MON_MOR","OSTETRIC","PEDIAT","PESO","PRESENZA","PRES_NEO","PROF_RH","PROF_RHNO","PROGFIGL","RIANIMAZ","RISAUT","SESSO","TRAVAGLIO","VENTILAZ","VITALITA_ARSNEW","DATAPARTO_ARSNEW") #"DATAPARTO_ARSNEW" added
# CAP200<-CAP20[ALLATTA!="" | ALTROSAN!="" | ANESTES!="" | APGAR!="" | CARIOTIPO!="" | CIRCOST!="" | CIRC_CRA!="" | DESMAL_1!="" | DESMAL_2!="" | D_CICOST!="" | D_MAL_1M!="" | D_MAL_2M!="" | D_MAL_GR1!="" | D_MAL_GR2!="" | ES_STRUM!="" | ES_STRUM!="" | ETA_G_MAL!="" | ETA_NEO!="" | FARM_TRAV!="" | FOTOGRAF!="" | GENITALI!="" | GINECOL!="" | INDIFARM_ARSNEW!="" | INDOTTO_ARSNEW!="" | INTUBAZ!="" | KRISTELLER!="" | LUNGH!="" | LUOGO!="" | MALFOR!="" | MALF_1!="" | MALF_2!="" | MALF_3!="" | MALF_FRA!="" | MALF_G_M!="" | MALF_G_P!="" | MALF_M!="" | MALF_P!="" | MALF_P_M!="" | MALF_P_P!="" | MAL_1!="" | MAL_1_M!="" | MAL_2!="" | MAL_2_M!="" | MAL_GR_1!="" | MAL_GR_2!="" | MOD_PART!="" | MON_MOR!="" | OSTETRIC!="" | PEDIAT!="" | PESO!="" | PRESENZA!="" | PRES_NEO!="" | PROF_RH!="" | PROF_RHNO!="" | PROGFIGL!="" | RIANIMAZ!="" | RISAUT!="" | SESSO!="" | TRAVAGLIO!="" | VENTILAZ!="" | VITALITA_ARSNEW!=""| DATAPARTO_ARSNEW!="",]
# 
# #14/04: add unique
# CAP201<-unique(CAP200,by=c("IDUNI","survey_id","DATAPARTO_ARSNEW"))
# 
# CAP21<-melt(CAP201, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value") 
# #SURVEY_OBSERVATIONS_CAP2<-CAP21[,survey_id:=paste0("SO_CAP2_",seq_along(IDUNI))]
# SURVEY_OBSERVATIONS_CAP2<-CAP21[,`:=`(so_source_table="CAP2",so_origin="CAP2",so_meaning="birth_registry_child")]
# SURVEY_OBSERVATIONS_CAP2<-SURVEY_OBSERVATIONS_CAP2[so_source_column=="PESO",so_unit:="g"][so_source_column=="LUNGHEZZA",so_unit:="cm"]
# SURVEY_OBSERVATIONS_CAP2<-SURVEY_OBSERVATIONS_CAP2[so_source_column=="MAL_1" |so_source_column=="MAL_2" | so_source_column=="MAL_1_M" |so_source_column=="MAL_2_M" | so_source_column=="MALF_1"|so_source_column=="MALF_2" |so_source_column=="MALF_3" |so_source_column=="CIRCOST" ,so_unit:="ICD9"]
# 
# ## duplicate DATAPARTO_ARSNEW, as it is and in "so_date"
# SURVEY_OBSERVATIONS_CAP2<-merge(SURVEY_OBSERVATIONS_CAP2,CAP2[,.(IDUNI,ID_CAP1_ARSNEW,DATAPARTO_ARSNEW)],by=c("IDUNI","ID_CAP1_ARSNEW"),all.x = T)
# 
# setnames(SURVEY_OBSERVATIONS_CAP2, old="IDUNI", new="person_id")
# setnames(SURVEY_OBSERVATIONS_CAP2, old="DATAPARTO_ARSNEW", new="so_date")
# SURVEY_OBSERVATIONS_CAP2[,`:=`(so_date=as.Date(so_date))]
# SURVEY_OBSERVATIONS_CAP2[,`:=`(so_date=format(so_date, "%Y%m%d"))]
# 
# SURVEY_OBSERVATIONS_CAP2<-SURVEY_OBSERVATIONS_CAP2[,.(person_id,so_date,so_source_table,so_source_column,so_source_value,so_unit,so_origin,so_meaning,survey_id)]
# 
# fwrite(SURVEY_OBSERVATIONS_CAP2, paste0(diroutput,"/SURVEY_OBSERVATIONS_CAP2.csv"), quote = "auto")
# rm(CAP20, CAP21)
# 
# rm(list=ls(pattern="^CAP"))

# NO - 9.  Use ABS to populate SURVEY_ID, then SURVEY_OBSERVATIONS -------------

# ##Specification table SURVEY_ID - ABS:
#   #-ABS:For each record
#     # ●	Create one record of SURVEY_ID and number the records with a sequential number stored in survey_id
#     # ●	Copy the values of ABS into SURVEY_ID according to the following table 
# 
# ABS <- as.data.table(read_dta(paste0(dirinput,"/ABS.dta")))
# setkeyv(ABS,"IDUNI")
# 
# SURVEY_ID_ABS<-ABS[,`:=`(survey_id=paste0("ABS_",seq_along(IDUNI)),
#                          survey_meaning="spontaneous_abortion_registry",
#                          survey_origin="ABS")]
# 
# setnames(SURVEY_ID_ABS,"IDUNI", "person_id")
# setnames(SURVEY_ID_ABS,"DATAINT", "survey_date")
# 
# SURVEY_ID_ABS<-SURVEY_ID_ABS[,.(person_id,survey_id,survey_date,survey_meaning,survey_origin)]
# 
# SURVEY_ID_ABS<-SURVEY_ID_ABS[,`:=`(survey_date=format(survey_date, "%Y%m%d"))]
# 
# fwrite(SURVEY_ID_ABS, paste0(diroutput,"/SURVEY_ID_ABS.csv"), quote = "auto")
# 
# setnames(ABS,"survey_date","DATAINT")
# setnames(ABS, "person_id","IDUNI")
# 
# ## Specification table SURVEY_OBSERVATION - ABS:
# #-ABS: For each record 
#     # ●	Extract from SURVEY_ID (above) the corresponding value of survey_id
#     # ●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below
# ABS<-ABS[,DATAINT:=as.character(DATAINT)]
# 
# vars<-c("ABORTI","ANONIMO","CAUSA","CITTU_ARSNEW","COMPLIC","CONDPROF","DEGENZA","ETAMADRE_ARSNEW","FIGLI","IVG","LUOGO","METODO","NATMORTI","NATVIVI","RAMATT","RIPRASS","SETTAMEN_ARSNEW","STATCIV","TERAPIA","TIPO","TITSTU","DATAINT") #DATAINT added
# SURVEY_OBSERVATIONS_ABS_<-ABS[ABORTI!="" | ANONIMO!="" | CAUSA!="" | CITTU_ARSNEW!="" | COMPLIC!="" | CONDPROF!="" | DEGENZA!="" | ETAMADRE_ARSNEW!="" | FIGLI!="" | IVG!="" | LUOGO!="" | METODO!="" | NATMORTI!="" | NATVIVI!="" | RAMATT!="" | RIPRASS!="" | SETTAMEN_ARSNEW!="" | STATCIV!="" | TERAPIA!="" | TIPO!="" | TITSTU!="" | !is.na(DATAINT),]
# 
# SURVEY_OBSERVATIONS_ABS<-melt(SURVEY_OBSERVATIONS_ABS_, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value") 
# 
# SURVEY_OBSERVATIONS_ABS<-SURVEY_OBSERVATIONS_ABS[,`:=`(so_unit="", 
#                                                        so_source_table="ABS",
#                                                        so_origin="ABS",
#                                                        so_meaning="spontaneous_abortion_registry")]
# ## duplicate DATAINT, as it is and in "so_date"
# SURVEY_OBSERVATIONS_ABS<-merge(SURVEY_OBSERVATIONS_ABS,ABS[,.(IDUNI,survey_id,DATAINT)],by=c("IDUNI","survey_id"),all.x = T)
# 
# setnames(SURVEY_OBSERVATIONS_ABS,"DATAINT","so_date")
# setnames(SURVEY_OBSERVATIONS_ABS,"IDUNI","person_id")
# 
# SURVEY_OBSERVATIONS_ABS[,`:=`(so_date=as.Date(so_date))]
# SURVEY_OBSERVATIONS_ABS[,`:=`(so_date=format(so_date, "%Y%m%d"))]
# 
# SURVEY_OBSERVATIONS_ABS<-SURVEY_OBSERVATIONS_ABS[,.(person_id,so_date,so_source_table,so_source_column,so_source_value,so_unit,so_origin,so_meaning,survey_id)]
# 
# fwrite(SURVEY_OBSERVATIONS_ABS, paste0(diroutput,"/SURVEY_OBSERVATIONS_ABS.csv"), quote = "auto")
# 
# rm(ABS)

# NO - 10. Use IVG to populate SURVEY_ID, then SURVEY_OBSERVATIONS ------------
# 
# ##Specification table SURVEY_ID - IVG:
#   #- IVG:For each record
#     # ●	Create one record of SURVEY_ID and number the records with a sequential number stored in survey_id
#     # ●	Copy the values of IVG into SURVEY_ID according to the following table 
# 
# IVG <- as.data.table(read_dta(paste0(dirinput,"/IVG.dta")))
# setkeyv(IVG,"IDUNI")
# 
# SURVEY_ID_IVG<-IVG[,survey_id:=paste0("IVG_",seq_along(IDUNI))]
# SURVEY_ID_IVG<-SURVEY_ID_IVG[,.(IDUNI,survey_id, DATAINT)]
# SURVEY_ID_IVG<-SURVEY_ID_IVG[,`:=`(survey_meaning="induced_termination_registry",
#                                    survey_origin="IVG")]
# 
# setnames(SURVEY_ID_IVG, old="IDUNI", new="person_id")
# setnames(SURVEY_ID_IVG, old="DATAINT", new="survey_date")
# 
# SURVEY_ID_IVG<-SURVEY_ID_IVG[,`:=`(survey_date=format(survey_date, "%Y%m%d"))]
# 
# fwrite(SURVEY_ID_IVG, paste0(diroutput,"/SURVEY_ID_IVG.csv"), quote = "auto")
# 
# ## Specification table SURVEY_OBSERVATION -IVG:
#   #-IVG: For each record 
#     # ●	Extract from SURVEY_ID (above) the corresponding value of survey_id
#     # ●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below
# IVG<-IVG[,DATAINT:=as.character(DATAINT)]
# 
# vars<-c("CONDPROF","POSPROF","RAMATT","NATVIVI","NATMORTI","FIGLI","CITTU_ARSNEW","MALFOR","DESCR_INTERV","PROSTAGLANDINE","COMPLIC0","COMPLIC1","COMPLIC2","COMPLIC3","COMPLIC4","COMPLIC0_ARSNEW","TITSTU_ARSNEW","STATCIV_ARSNEW","NATVIV_ARSNEW","NATMORT_ARSNEW","ABORTI_ARSNEW","IVG_ARSNEW","SETTAMEN","CERTIF","URGENZA","ASSENSO","LUOGO","TIPO","TERAPIA","DEGENZA","COMPLIC","ETAMADRE_ARSNEW","ETAGEST_ARSNEW","DATAINT") #"DATAINT" added
# SURVEY_OBSERVATIONS_IVG_<-IVG[CONDPROF!="" | POSPROF!="" | RAMATT!="" | !is.na(NATVIVI) | !is.na(NATMORTI) | !is.na(FIGLI) | CITTU_ARSNEW!="" | MALFOR!="" | DESCR_INTERV!="" | !is.na(PROSTAGLANDINE) | COMPLIC0!="" | COMPLIC1!="" | COMPLIC2!="" | COMPLIC3!="" | COMPLIC4!="" | COMPLIC0_ARSNEW!="" | TITSTU_ARSNEW!="" | STATCIV_ARSNEW!="" | NATVIV_ARSNEW!="" | NATMORT_ARSNEW!="" | ABORTI_ARSNEW!="" | IVG_ARSNEW!="" | !is.na(SETTAMEN) | CERTIF!="" | URGENZA!="" | ASSENSO!="" | LUOGO!="" | TIPO!="" | TERAPIA!="" | !is.na(DEGENZA) | COMPLIC!="" | !is.na(ETAMADRE_ARSNEW) | ETAGEST_ARSNEW!="" | DATAINT!="",]
# 
# SURVEY_OBSERVATIONS_IVG<-melt(SURVEY_OBSERVATIONS_IVG_, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value") 
# 
# SURVEY_OBSERVATIONS_IVG<-SURVEY_OBSERVATIONS_IVG[,`:=`(so_unit="",
#                                                        so_source_table="IVG",
#                                                        so_origin="IVG",
#                                                        so_meaning="induced_termination_registry")]
# ## duplicate DATAINT, as it is and in "so_date"
# SURVEY_OBSERVATIONS_IVG<-merge(SURVEY_OBSERVATIONS_IVG,IVG[,.(IDUNI,survey_id,DATAINT)],by=c("IDUNI","survey_id"),all.x = T)
# 
# setnames(SURVEY_OBSERVATIONS_IVG,"IDUNI","person_id")
# setnames(SURVEY_OBSERVATIONS_IVG,"DATAINT","so_date")
# 
# SURVEY_OBSERVATIONS_IVG<-SURVEY_OBSERVATIONS_IVG[,.(person_id,so_date,so_source_table,so_source_column,so_source_value,so_unit,so_origin,so_meaning,survey_id)]
# 
# SURVEY_OBSERVATIONS_IVG[,`:=`(so_date=as.Date(so_date))]
# SURVEY_OBSERVATIONS_IVG[,`:=`(so_date=format(so_date, "%Y%m%d"))]
# 
# fwrite(SURVEY_OBSERVATIONS_IVG, paste0(diroutput,"/SURVEY_OBSERVATIONS_IVG.csv"), quote = "auto")
# 
# rm(IVG)

# 11. Use RMR to populate REGISTRO_DI_MORTE  ------------

RMR <- as.data.table(read_dta(paste0(dirinput,"/RMR.dta")))
setkeyv(RMR,"IDUNI")

REGISTRO_DI_MORTE<-RMR[,survey_id:=paste0("RMR_",seq_along(IDUNI))]

setnames(REGISTRO_DI_MORTE, old="IDUNI", new="id")
setnames(REGISTRO_DI_MORTE, old="DATMORTE", new="datadec")
setnames(REGISTRO_DI_MORTE, old="CAUSAMORTE", new="causadec")


REGISTRO_DI_MORTE<-REGISTRO_DI_MORTE[,.(id,datadec,causadec)]

fwrite(REGISTRO_DI_MORTE, paste0(diroutput,"/REGISTRO_DI_MORTE_RMR.csv"), quote = "auto")

rm(RMR)

# NO - 12. Use AP to populate VISIT_OCCURRENCE, then MEDICAL_OBSERVATIONS  ----------------------------
# 
# ##Specification table VISIT_OCCURRENCE - AP:
#   #-AP: For each record 
#     # ●	Create a record of VISIT_OCCURRENCE and label the records with a unique code stored in visit_occurrence_id (primary key)
#     # ●	Copy the values of AP into VISIT_OCCURRENCE according to the following table
# 
# AP <- as.data.table(read_dta(paste0(dirinput,"/AP.dta")))
# setkeyv(AP,"IDUNI")
# 
# ## 2/12: drop 2 first character if they are letters in COD_MORF_1, COD_MORF_2, COD_MORF_3, COD_TOPOG
# AP<-AP[COD_MORF_1=="00000000" | COD_MORF_1=="000000000" | COD_MORF_1=="0000", COD_MORF_1:=NA]
# AP<-AP[COD_MORF_2=="00000000" | COD_MORF_2=="000000000" | COD_MORF_2=="0000", COD_MORF_2:=NA]
# AP<-AP[COD_MORF_3=="00000000" | COD_MORF_3=="000000000" | COD_MORF_3=="0000", COD_MORF_3:=NA]
# AP<-AP[COD_TOPOG=="00000000" | COD_TOPOG=="000000000" | COD_TOPOG=="0000", COD_TOPOG:=NA]
# 
# 
# VISIT_OCCURRENCE_AP<-AP[,`:=`(visit_occurrence_id=paste0("AP_",seq_along(DAT_ACC)))][,.(IDUNI, DAT_ACC, visit_occurrence_id)]
# VISIT_OCCURRENCE_AP<-VISIT_OCCURRENCE_AP[,`:=`(visit_end_date="",specialty_of_visit="",specialty_of_visit_vocabulary="",status_at_discharge="",status_at_discharge_vocabulary="",meaning_of_visit="pathology_report",origin_of_visit="AP")]
# 
# setnames(VISIT_OCCURRENCE_AP, "IDUNI", "person_id")
# setnames(VISIT_OCCURRENCE_AP, "DAT_ACC", "visit_start_date")
# 
# VISIT_OCCURRENCE_AP<-VISIT_OCCURRENCE_AP[,`:=`(visit_start_date=format(visit_start_date, "%Y%m%d"))]
# 
# fwrite(VISIT_OCCURRENCE_AP, paste0(diroutput,"/VISIT_OCCURRENCE_AP.csv"), quote = "auto")
# 
# ## Specification table MEDICAL_OBSERVATIONS - AP:
#   #-AP: For each record
#     # ●	Extract from VISIT_OCCURRENCE (above) the corresponding value of visit_occurrence_id
#     # ●	Create a record of MEDICAL_OBSERVATIONS for each non-empty column of this list
#       # a.	Columns containing codes
#         # i.	COD_MORF_1, 
#         # ii.	COD_MORF_2, 
#         # iii.COD_MORF_3,
#         # iv.	COD_TOPOG
#       # b.	Columns containing text:
#         # i.	DIAGNOSI
#         # ii.	MACROSCOPIA
#     # ●	Copy the values of AP into each such record of MEDICAL_OBSERVATIONS according to the following table 
# 
# MEDICAL_OBSERVATIONS_<-AP[(COD_MORF_1!="" | COD_MORF_2!="" | COD_MORF_3!="" | COD_TOPOG!="") | (DIAGNOSI!="" | MACROSCOPIA!="") ,]
# 
# MEDICAL_OBSERVATIONS_W<-melt(MEDICAL_OBSERVATIONS_, measure.vars = c("COD_MORF_1","COD_MORF_2","COD_MORF_3","COD_TOPOG", "MACROSCOPIA","DIAGNOSI"), variable.name = "mo_source_column", value.name = "mo_source_value")[mo_source_value!="",]
# #keys<-c("IDUNI","visit_occurrence_id");setkeyv(MEDICAL_OBSERVATIONS_W,keys)
# 
# MEDICAL_OBSERVATIONS_W<-MEDICAL_OBSERVATIONS_W[mo_source_column=="COD_MORF_1" | mo_source_column=="COD_MORF_2" | mo_source_column=="COD_MORF_3" | mo_source_column=="COD_TOPOG",mo_source_column:=""]
# MEDICAL_OBSERVATIONS_W<-MEDICAL_OBSERVATIONS_W[mo_source_column=="",mo_code:=mo_source_value]
# MEDICAL_OBSERVATIONS_W<-MEDICAL_OBSERVATIONS_W[mo_source_column=="",`:=`(mo_record_vocabulary="SNOMED3",mo_source_value="")]
# 
# MEDICAL_OBSERVATIONS_AP<-MEDICAL_OBSERVATIONS_W[,`:=`(mo_source_table="AP", mo_unit="",mo_meaning="pathology_report",mo_origin="AP")]
# rm(MEDICAL_OBSERVATIONS_, MEDICAL_OBSERVATIONS_W)
# 
# setnames(MEDICAL_OBSERVATIONS_AP, "IDUNI", "person_id")
# setnames(MEDICAL_OBSERVATIONS_AP, "DAT_ACC", "mo_date")
# 
# MEDICAL_OBSERVATIONS_AP<-MEDICAL_OBSERVATIONS_AP[,`:=`(mo_date=as.Date(mo_date, "%d%b%Y"))] #10/09:added
# MEDICAL_OBSERVATIONS_AP<-MEDICAL_OBSERVATIONS_AP[,`:=`(mo_date=format(mo_date, "%Y%m%d"))]
# 
# fwrite(MEDICAL_OBSERVATIONS_AP, paste0(diroutput,"/MEDICAL_OBSERVATIONS_AP.csv"), quote = "auto")
# 
# rm(AP)

# NO - 13. Use VCN to populate VACCINES ----------------------------------------
# 
# ## Specification table VACCINES - VCN: 
# #The local tables feeding this CDM table are: VCN
# 
# VCN <- as.data.table(read_dta(paste0(dirinput,"/VCN.dta")))
# setkeyv(VCN,"IDUNI")
# 
# VACCINES<-VCN[,`:=`(vx_type="",vx_text="", visit_occurrence_id="",origin_of_vx_record="VCN",meaning_of_vx_record="administration_of_vaccine_unspecified",vx_record_date=DATA_SOMMINISTRAZIONE, vx_admin_date=DATA_SOMMINISTRAZIONE)]
# 
# setnames(VACCINES, "IDUNI", "person_id")
# #setnames(VACCINES, "DATA_SOMMINISTRAZIONE", "vx_record_date")	
# #setnames(VACCINES, "DATA_INIZIO", "vx_record_date")
# #setnames(VACCINES, "DATA_SOMMINISTRAZIONE", "vx_admin_date")
# setnames(VACCINES, "COD_ATC5", "vx_atc")
# #setnames(VACCINES, "COD_ATC_VACCINO", "vx_atc")
# #setnames(VACCINES, "COD_PREST_VACCINO", "product_code")
# setnames(VACCINES, "COD_PREST_VACCINO", "medicinal_product_id")
# setnames(VACCINES, "DOSE_SOMMINISTRATA", "vx_dose")	
# setnames(VACCINES, "TITOLARE_AIC", "vx_manufacturer")	
# setnames(VACCINES, "NUM_LOTTO", "vx_lot_num")
# 
# VACCINES<-VACCINES[,.(person_id,vx_record_date,vx_admin_date,vx_atc,vx_type,vx_text,medicinal_product_id, origin_of_vx_record,meaning_of_vx_record,vx_dose,vx_manufacturer,vx_lot_num,visit_occurrence_id)] #product_code
# 
# VACCINES<-VACCINES[,`:=`(vx_record_date=format(vx_record_date, "%Y%m%d"), vx_admin_date=format(vx_admin_date, "%Y%m%d"))]
# VACCINES<-VACCINES[,`:=`(medicinal_product_id=as.character(medicinal_product_id))]
# 
# fwrite(VACCINES, paste0(diroutput,"/VACCINES.csv"), quote = "auto")
# 
# rm(VCN,VACCINES)

# NO - 14.	Use COD_FARMACI_SPF to populate PRODUCTS ----------------------------
# 
# ## Specification table PRODUCTS -COD_FARMACI_SPF: 
# #The origin tables feeding this target CDM table are: COD_FARMACI_SPF
# 
# COD_FARMACI_SPF <- as.data.table(read_dta(paste0(dirinput,"/COD_FARMACI_SPF.dta")))
# 
# #setkeyv(COD_FARMACI_SPF,"IDUNI")
# 
# PRODUCTS<-COD_FARMACI_SPF
# PRODUCTS<-PRODUCTS[,`:=`( drug_form="",ingredient1_ATCcode="",ingredient2_ATCcode="",ingredient3_ATCcode="",amount_ingredient1="",amount_ingredient2="",amount_ingredient3="",amount_ingredient1_unit="", amount_ingredient2_unit="",amount_ingredient3_unit="")] #box_size="",box_size_unit="",
# 
# setnames(PRODUCTS,"COD_PRESTAZIONE","product_code")
# setnames(PRODUCTS,"DESCRIZIONE","full_product_name")
# setnames(PRODUCTS,"UNITA_POSOLOGIA","box_size")
# setnames(PRODUCTS,"UM","box_size_unit")
# setnames(PRODUCTS,"VDS","route_of_administration")
# setnames(PRODUCTS,"COD_ATC5","product_ATCcode")
# setnames(PRODUCTS,"TITOLARE_AIC","product_manufacturer")
# 
# # drop missing ATC!!!
# PRODUCTS<-PRODUCTS[product_ATCcode!="",]
# 
# PRODUCTS<-PRODUCTS[,.(product_code,full_product_name,box_size,box_size_unit,drug_form,route_of_administration,product_ATCcode,ingredient1_ATCcode,ingredient2_ATCcode,ingredient3_ATCcode,amount_ingredient1, amount_ingredient2, amount_ingredient3, amount_ingredient1_unit,amount_ingredient2_unit,amount_ingredient3_unit,product_manufacturer)] 
# 
# fwrite(PRODUCTS, paste0(diroutput,"/PRODUCTS.csv"), quote = "auto")
# 
# rm(COD_FARMACI_SPF)

# NO - 15.	Use CAP to populate PERSON_RELATIONSHIP -----------------------------
# 
# ## Specification table PERSON_RELATIONSHIPS - CAP2: 
# #The origin tables feeding this target CDM table are: CAP2
# 
#   #- CAP2:For each record of each child, perform record linkage with CAP1 and fill the column of this table as follows
# 
# for (source in cap){
#   pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
#   assign(source,pippo)
#   setkeyv(pippo,"IDUNI")
# }
# rm(pippo)
# 
# PERSON_RELATIONSHIPS_<-merge(CAP2[,.(IDUNI,ID_CAP1_ARSNEW)], CAP1[,.(IDUNI,ID_CAP1_ARSNEW)], by="ID_CAP1_ARSNEW")
# 
# PERSON_RELATIONSHIPS_<-PERSON_RELATIONSHIPS_[,`:=`(origin_of_relationship="birth_registry",
#   meaning_of_relationship="gestational_mother",method_of_linkage="deterministic")]
# 
# setnames(PERSON_RELATIONSHIPS_, "IDUNI.x","person_id")
# setnames(PERSON_RELATIONSHIPS_, "IDUNI.y","related_id")
# 
# PERSON_RELATIONSHIPS<-PERSON_RELATIONSHIPS_[,.(person_id,related_id,origin_of_relationship,meaning_of_relationship,method_of_linkage)]
# 
# fwrite(PERSON_RELATIONSHIPS, paste0(diroutput,"/PERSON_RELATIONSHIPS.csv"), quote = "auto")
# 
# rm(PERSON_RELATIONSHIPS_, CAP2, CAP1)

# 16.	Use ARS_ANAG_MED_RES_storico to populate ANAGRAFE_ASSISTITI --------

ANA <- as.data.table(read_dta(paste0(dirinput,"/ANA.dta")))
setkeyv(ANA,"IDUNI")


ANAGRAFE_ASSISTITI<-ANA

setnames(ANAGRAFE_ASSISTITI, "IDUNI","id")
setnames(ANAGRAFE_ASSISTITI, "DATA_NASCITA_D","datanas")
setnames(ANAGRAFE_ASSISTITI, "DATA_MORTE_MARSI","datadec")
setnames(ANAGRAFE_ASSISTITI, "SESSO","sesso")
setnames(ANAGRAFE_ASSISTITI, "INIZIO","data_inizioass")
setnames(ANAGRAFE_ASSISTITI, "FINE","data_fineass")

ANAGRAFE_ASSISTITI<-ANAGRAFE_ASSISTITI[,.(id,sesso,datanas,data_inizioass,data_fineass,datadec)]

fwrite(ANAGRAFE_ASSISTITI, paste0(diroutput,"/ANAGRAFE_ASSISTITI.csv"), quote = "auto")

rm(ANA)


# NO - 17. Use COVID_DATASET to populate  SURVEY_ID, SURVEY_OBSERVATION -------------------------------------

# ## Specification table SURVEY_ID:
# # For each record
# #●	Create one record of SURVEY_ID for each row of COVIDDATASET, 
# #●	Copy the values of COVIDDATASET into SURVEY_ID according to the following table for each maternal record
# 
# COVIDDATASET <- as.data.table(read_dta(paste0(dirinput,"COVIDDATASET.dta")))
# setkeyv(COVIDDATASET,"IDUNI")
# 
# # select variables of interest
# SURVEY_ID<-COVIDDATASET[,.(IDUNI, ID_CASO, DATA_PRELIEVO)]
# # changed format in date
# SURVEY_ID<-SURVEY_ID[,DATA_PRELIEVO:=as.Date(DATA_PRELIEVO)]
# # add var according to CDM
# SURVEY_ID<-SURVEY_ID[,survey_meaning:="covid_registry"]
# 
# # rename vars
# setnames(SURVEY_ID, old="IDUNI", new = "person_id")
# setnames(SURVEY_ID, old="DATA_PRELIEVO", new = "survey_date")
# setnames(SURVEY_ID, old="ID_CASO", new = "survey_id")
# 
# SURVEY_ID<-SURVEY_ID[,`:=`(survey_date= format(survey_date, "%Y%m%d"))]
# 
# fwrite(SURVEY_ID, paste0(diroutput,"SURVEY_ID_COVIDDATASET.csv"), quote = "auto")
# 
# 
# 
# ## Specification table SURVEY_OBSERVATION:
# 
# #For each record 
# #●	Extract from SURVEY_ID (above) the corresponding value of survey_id
# #●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below; 
# # a.	DATA_INIZIO_SINTOMI
# # b.	DATA_PRELIEVO
# # c.	DATA_DIAGNOSI
# # d.	STATOCLINICO_PRELIEVO
# # e.	STATOCLINICO_PIU_GRAVE
# # f.	STATOCLINICO_PIU_RECENTE
# # g.	DATA_STATOCLINICO_PIU_RECENTE
# # h.	CASO_ATTIVO
# # i.	RICOVERO
# #for each of them, copy the content of the cell in the column obsevartion_source_value, and copy the other values as follows
# 
# # COVIDDATASET<-COVIDDATASET[,DATA_INIZIO_SINTOMI:=as.Date(DATA_INIZIO_SINTOMI)]
# # COVIDDATASET<-COVIDDATASET[,DATA_DIAGNOSI:=as.Date(DATA_DIAGNOSI)]
# # COVIDDATASET<-COVIDDATASET[,DATA_STATOCLINICO_PIU_RECENTE:=as.Date(DATA_STATOCLINICO_PIU_RECENTE)]
# 
# SURVEY_OBSERVATIONS<-COVIDDATASET[,.(IDUNI, ID_CASO, DATA_PRELIEVO)][,so_source_table:="COVIDDATASET"]
# 
# # change format of date in chr, to melt data
# COVIDDATASET<-COVIDDATASET[,DATA_PRELIEVO:=as.character(DATA_PRELIEVO)]
# COVIDDATASET<-COVIDDATASET[,DATA_INIZIO_SINTOMI:=as.character(DATA_INIZIO_SINTOMI)]
# COVIDDATASET<-COVIDDATASET[,DATA_DIAGNOSI:=as.character(DATA_DIAGNOSI)]
# COVIDDATASET<-COVIDDATASET[,DATA_STATOCLINICO_PIU_RECENTE:=as.character(DATA_STATOCLINICO_PIU_RECENTE)]
# 
# vars<- c("DATA_INIZIO_SINTOMI", "DATA_PRELIEVO", "DATA_DIAGNOSI",
#          "STATOCLINICO_PRELIEVO", "STATOCLINICO_PIU_GRAVE", "STATOCLINICO_PIU_RECENTE",
#          "DATA_STATOCLINICO_PIU_RECENTE", "CASO_ATTIVO", "RICOVERO")
# 
# COVIDDATASET_noempty<- COVIDDATASET[ !is.na(DATA_INIZIO_SINTOMI)| !is.na(DATA_PRELIEVO)| !is.na(DATA_DIAGNOSI)| STATOCLINICO_PRELIEVO!=""| STATOCLINICO_PIU_GRAVE!=""| STATOCLINICO_PIU_RECENTE!=""| !is.na(DATA_STATOCLINICO_PIU_RECENTE)| CASO_ATTIVO!=""| RICOVERO!=""| ID_CASO!="",]
# 
# COVIDDATASET_melt<-melt(COVIDDATASET_noempty, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value")
# 
# SURVEY_OBSERVATIONS_CD<-merge(COVIDDATASET_melt, SURVEY_OBSERVATIONS, by=c("IDUNI","ID_CASO"), all.x = T)
# 
# setnames(SURVEY_OBSERVATIONS_CD, old="IDUNI", new = "person_id")
# setnames(SURVEY_OBSERVATIONS_CD, old="DATA_PRELIEVO", new = "so_date")
# setnames(SURVEY_OBSERVATIONS_CD, old="ID_CASO", new = "survey_id")
# 
# SURVEY_OBSERVATIONS_CD<-SURVEY_OBSERVATIONS_CD[,so_meaning:="covid_registry"]
# SURVEY_OBSERVATIONS_CD<-SURVEY_OBSERVATIONS_CD[,`:=`(so_date= format(so_date, "%Y%m%d"),so_unit="")]
# 
# SURVEY_OBSERVATIONS_CD<-SURVEY_OBSERVATIONS_CD[,.(person_id,so_date,so_source_table,so_source_column,so_source_value,survey_id)]
# 
# fwrite(SURVEY_OBSERVATIONS_CD, paste0(diroutput,"SURVEY_OBSERVATIONS_COVIDDATASET.csv"), quote = "auto")
# 
# rm(COVIDDATASET, COVIDDATASET_melt, COVIDDATASET_noempty, SURVEY_ID, SURVEY_OBSERVATIONS_CD, SURVEY_OBSERVATIONS)
# 
# 
# 

