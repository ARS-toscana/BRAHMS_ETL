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



# 1.	Use SDO to populate VISIT_OCCURRENCE, then EVENTS, PROCEDURES and MEDICAL_OBSERVATIONS, PERSONS and OBSERVATIONS_PERIODS --------

SDO <- as.data.table(read_dta(paste0(dirinput,"/SDO.dta"))) # fare dirinput!!
setkeyv(SDO,"IDUNI") 

## Specification table: VISIT_OCCURRENCE- SDO
# The local tables feeding this CDM table are: SDO, SPA, AP. The rules to create VISIT_OCCURRENCE from them are as follows:
  #- SDO:
    # For each record
    # ●	Create a record of VISIT_OCCURRENCE and number the records with a sequential number stored in visit_occurrence_id
    # ●	Copy the values of SDO into VISIT_OCCURRENCE according to the following table

# transform and create variable:
## nometabella_"numero"
VISIT_OCCURRENCE_<- SDO[,`:=`(visit_occurrence_id=paste0("SDO_",seq_along(DATDIM)))]
VISIT_OCCURRENCE_<-VISIT_OCCURRENCE_[,`:=`(specialty_of_visit=REPDIM,
                                           specialty_of_visit_vocabulary="SDO.REPDIM",
                                           status_at_discharge=MODIM,
                                           status_at_discharge_vocabulary="SDO.MODIM",
                                           origin_of_visit="SDO")]
VISIT_OCCURRENCE_<-VISIT_OCCURRENCE_[REGIME==2, meaning_of_visit:="hospitalisation_not_overnight"] 
VISIT_OCCURRENCE_<-VISIT_OCCURRENCE_[REGIME!=2, meaning_of_visit:="hospitalisation"]
dim(SDO);SDO<-SDO[,-39:-43]

# keep only which needed
VISIT_OCCURRENCE <- VISIT_OCCURRENCE_[,.(IDUNI,visit_occurrence_id,DATAMM,DATDIM,specialty_of_visit,status_at_discharge,status_at_discharge_vocabulary,meaning_of_visit, origin_of_visit)]

#modification of REPDIM
#VISIT_OCCURRENCE <-VISIT_OCCURRENCE[,REPDIM:=substr(REPDIM,1,2)]

# rename variables:
setnames(VISIT_OCCURRENCE, old = "IDUNI", new = "person_id")
setnames(VISIT_OCCURRENCE, old = "DATAMM", new = "visit_start_date")
setnames(VISIT_OCCURRENCE, old = "DATDIM", new = "visit_end_date")
#setnames(VISIT_OCCURRENCE, old = "REPDIM", new = "specialty_of_visit")
#setnames(VISIT_OCCURRENCE, old = "MODIM", new = "status_at_discharge")
rm(VISIT_OCCURRENCE_)

VISIT_OCCURRENCE<-VISIT_OCCURRENCE[,`:=`(visit_start_date=format(visit_start_date, "%Y%m%d"), visit_end_date=format(visit_end_date, "%Y%m%d"))]

fwrite(VISIT_OCCURRENCE, paste0(diroutput,"VISIT_OCCURRENCE_SDO.csv"), quote = "auto")


## Specification table: EVENTS - SDO
# The local tables feeding this CDM table are: SDO, SEA, PS, SALM, RMR. The rules to create EVENTS from each of them are as follows:
  #- SDO:
    # For each record
    # ●	Extract from VISIT_OCCURRENCE (above) the corresponding value of visit_occurrence_id
    # ●	Create a record of EVENTS for each non-empty cell among DIADIM, PAT1,...,PAT5; for each of them, copy the content of the cell in the column code_event, and copy 
    # ●	Copy the values of SDO into EVENTS according to the following table

VISIT_OCCURRENCE_<- SDO[,visit_occurrence_id:=paste0("SDO_",seq_along(DATDIM))][,.(IDUNI,DATAMM,DATDIM,DIADIM,PAT1,PAT2,PAT3,PAT4,PAT5,DIADIM_PRES,DIA1_PRES,DIA2_PRES,DIA3_PRES,DIA4_PRES,DIA5_PRES,visit_occurrence_id)]
#dim(SDO); SDO<-SDO[,-39]
setnames(VISIT_OCCURRENCE_, old = "DIADIM", new = "PAT0")
setnames(VISIT_OCCURRENCE_, old = "DIADIM_PRES", new = "DIA0_PRES")
#VISIT_OCCURRENCE_<-VISIT_OCCURRENCE_[,N_DIA:= (DIADIM!="")+(PAT1!="")+(PAT2!="")+(PAT3!="")+(PAT4!="")+(PAT5!=""),by=c("IDUNI","visit_occurrence_id")] [,.(IDUNI,DATAMM,DATDIM,DIADIM,PAT1,PAT2,PAT3,PAT4,PAT5,visit_occurrence_id,N_DIA)]
EVENTS_<-melt(VISIT_OCCURRENCE_, measure= patterns("^PAT","^DIA"), value.name = c("event_code", "present_on_admission"), variable.name = "ord") [event_code!="",]
keys<-c("IDUNI","visit_occurrence_id");setkeyv(EVENTS_,keys)

EVENTS<-EVENTS_[present_on_admission==1,present_on_admission:="si"]
EVENTS<-EVENTS_[present_on_admission=="",present_on_admission:="no"]

EVENTS<-EVENTS[,event_record_vocabulary:="ICD9"]
EVENTS<-EVENTS[,`:=`(text_linked_to_event_code="",
  event_free_text="", 
  origin_of_event="SDO", laterality_of_event="")]
EVENTS<-EVENTS[ord==1,meaning_of_event:="hospitalisation_primary"]
EVENTS<-EVENTS[ord!=1,meaning_of_event:="hospitalisation_secondary"]

## cambiare present on event in SI/NO
# keep only which needed
EVENTS <- EVENTS[,.(IDUNI,DATAMM,DATDIM,event_code,event_record_vocabulary,text_linked_to_event_code,event_free_text,present_on_admission,laterality_of_event,meaning_of_event,origin_of_event,visit_occurrence_id)]

# rename variables:
setnames(EVENTS, old = "IDUNI", new = "person_id")
setnames(EVENTS, old = "DATAMM", new = "start_date_record")
setnames(EVENTS, old = "DATDIM", new = "end_date_record")
rm(VISIT_OCCURRENCE_, EVENTS_)

EVENTS<-EVENTS[,`:=`(start_date_record=format(start_date_record, "%Y%m%d"), end_date_record=format(end_date_record, "%Y%m%d"))]

fwrite(EVENTS, paste0(diroutput,"/EVENTS_SDO.csv"), quote = "auto")

## Specification table: PROCEDURES - SDO
# The local tables feeding this CDM table are: SDO, SPA
  #- SDO:
    # For each record
    # ●	Extract from VISIT_OCCURRENCE (above) the corresponding value of visit_occurrence_id
    # ●	Create a record of EVENTS for each non-empty cell among CODCHI, CODCHI2,...,CODCHI6; for each of them, copy the content of the cell in the column code_event, and copy 
    # ●	Copy the values of SDO into EVENTS according to the following table

VISIT_OCCURRENCE__<- SDO[,visit_occurrence_id:=paste0("SDO_",seq_along(DATDIM))] [,.(IDUNI,DATCHI,CODCHI,DATCHI2,CODCHI2,DATCHI3,CODCHI3,DATCHI4,CODCHI4,DATCHI5,CODCHI5, DATCHI6,CODCHI6,visit_occurrence_id)]
#VISIT_OCCURRENCE__<-VISIT_OCCURRENCE__[,N_DIA:= (CODCHI!="")+(CODCHI2!="")+(CODCHI3!="")+(CODCHI4!="")+(CODCHI5!="")+(CODCHI6!=""),by=c("IDUNI","visit_occurrence_id")] [,.(IDUNI,DATCHI,CODCHI,DATCHI2,CODCHI2,DATCHI3,CODCHI3,DATCHI4,CODCHI4,DATCHI5,CODCHI5, DATCHI6,CODCHI6,visit_occurrence_id,N_DIA)]

PROCEDURES_<-melt(VISIT_OCCURRENCE__, measure = patterns("^CODCHI","^DATCHI"), value.name = c("CODCHI", "DATCHI"), variable.name = "ord") 
keys<-c("IDUNI","visit_occurrence_id");setkeyv(PROCEDURES_,keys)

# keep only which needed
PROCEDURES_<-PROCEDURES_[CODCHI!="",]
PROCEDURES<- PROCEDURES_[,`:=`(procedure_code_vocabulary="ICD9PROC",
  meaning_of_procedure="procedure_during_hospitalisation",
  origin_of_procedure="SDO")]
  
# rename variables:
setnames(PROCEDURES, old = "IDUNI", new = "person_id")
setnames(PROCEDURES, old = "DATCHI", new = "procedure_date")
setnames(PROCEDURES, old = "CODCHI", new = "procedure_code")
rm(VISIT_OCCURRENCE__,PROCEDURES_)

PROCEDURES<-PROCEDURES[,.(person_id,procedure_date, procedure_code, procedure_code_vocabulary, visit_occurrence_id, meaning_of_procedure, origin_of_procedure)]

PROCEDURES<-PROCEDURES[,`:=`(procedure_date=format(procedure_date, "%Y%m%d"))]

fwrite(PROCEDURES, paste0(diroutput,"/PROCEDURES_SDO.csv"), quote = "auto")


## Specification table: MEDICAL_OBSERVATIONS - SDO
#The local tables feeding this CDM table are: SDO, AP, SALM

  #- SDO:For each record
    # ●	Extract from VISIT_OCCURRENCE (above) the corresponding value of visit_occurrence_id
    # ●	Create a record of MEDICAL_OBSERVATIONS for each column of this list
    # a.	istruz
    # b.	creatinina
    # ●	Copy the values of SDO into each such record of MEDICAL_OBSERVATIONS according to the following table

VISIT_OCCURRENCE_<- SDO[,visit_occurrence_id:=paste0("SDO_",seq_along(DATDIM))] [,.(IDUNI,DATAMM,TITSTU,CREATININA,visit_occurrence_id)]
#dim(SDO); SDO<-SDO[,-39]

MEDICAL_OBSERVATIONS_<-melt(VISIT_OCCURRENCE_,measure = c("TITSTU","CREATININA"),value.name = "mo_source_value", variable.name = "mo_source_column") 
keys<-c("IDUNI","visit_occurrence_id");setkeyv(MEDICAL_OBSERVATIONS_,keys)

# 30/09: change format of float variable
MEDICAL_OBSERVATIONS_<-MEDICAL_OBSERVATIONS_[,mo_source_value:=gsub(",",".",mo_source_value)]

# keep only which needed
MEDICAL_OBSERVATIONS_<-MEDICAL_OBSERVATIONS_[mo_source_value!="",]
MEDICAL_OBSERVATIONS_<- MEDICAL_OBSERVATIONS_[,`:=`(mo_source_table="SDO",
  mo_code="",
  mo_record_vocabulary="",
  mo_meaning="measure_during_hospitalisation",
  mo_origin="SDO")]
MEDICAL_OBSERVATIONS<-MEDICAL_OBSERVATIONS_[mo_source_table=="CREATININA",mo_unit:="mg/dL"]

# rename variables:
setnames(MEDICAL_OBSERVATIONS, old = "IDUNI", new = "person_id")
setnames(MEDICAL_OBSERVATIONS, old = "DATAMM", new = "mo_date")
rm(VISIT_OCCURRENCE_,MEDICAL_OBSERVATIONS_)

MEDICAL_OBSERVATIONS<-MEDICAL_OBSERVATIONS[,.(person_id,mo_date,mo_code,mo_record_vocabulary, mo_source_table,  mo_source_column, mo_source_value,mo_unit,mo_meaning, mo_origin,visit_occurrence_id)]

MEDICAL_OBSERVATIONS<-MEDICAL_OBSERVATIONS[,`:=`(mo_date=format(mo_date, "%Y%m%d"))]

fwrite(MEDICAL_OBSERVATIONS, paste0(diroutput,"/MEDICAL_OBSERVATIONS_SDO.csv"), quote = "auto")

## Specification table: PERSONS - SDO
#Select all the distinct values of IDUNI from SDO with PESONASC not missing and not in ARS_ANAG_MED_RES_storico; for each of them select the values as follows
VISIT_OCCURRENCE_<- SDO[,visit_occurrence_id:=paste0("SDO_",seq_along(DATDIM))]

## Specification table: OBSERVATION_PERIODS - SDO
#For each record with PESONASC not missing, fill the columns of this table as follows 
VISIT_OCCURRENCE_<- SDO[,visit_occurrence_id:=paste0("SDO_",seq_along(DATDIM))]


rm(SDO)



# 1a. Use SDOTEMP to populate VISIT_OCCURRENCE, then EVENTS, PROCEDURES and MEDICAL_OBSERVATIONS,PERSONS and OBSERVATIONS_PERIODS --------

SDOTEMP <- as.data.table(read_dta(paste0(dirinput,"/SDOTEMP.dta")))
setkeyv(SDOTEMP,"IDUNI")

## Specification table: VISIT_OCCURRENCE- SDO
# The local tables feeding this CDM table are: SDO, SPA, AP. The rules to create VISIT_OCCURRENCE from them are as follows:
  #- SDO:
  # For each record
    # ●	Create a record of VISIT_OCCURRENCE and number the records with a sequential number stored in visit_occurrence_id
    # ●	Copy the values of SDO into VISIT_OCCURRENCE according to the following table

# transform and create variable:
## nometabella_"numero"
VISIT_OCCURRENCE_<- SDOTEMP[,`:=`(visit_occurrence_id=paste0("SDOTEMP_",seq_along(DATDIM)))]
VISIT_OCCURRENCE_<-VISIT_OCCURRENCE_[,`:=`(specialty_of_visit_vocabulary="SDO.REPDIM", 
  status_at_discharge_vocabulary="SDO.MODIM",
  origin_of_visit="administrative_record")]
VISIT_OCCURRENCE_<-VISIT_OCCURRENCE_[REGIME==2, meaning_of_visit:="hospitalisation_not_overnight"] 
VISIT_OCCURRENCE_<-VISIT_OCCURRENCE_[REGIME!=2, meaning_of_visit:="hospitalisation"]
dim(SDOTEMP);SDOTEMP<-SDOTEMP[,-34:-38]

# keep only which needed
VISIT_OCCURRENCE <- VISIT_OCCURRENCE_[,.(IDUNI,visit_occurrence_id,DATAMM,DATDIM,REPDIM,specialty_of_visit_vocabulary,MODIM,status_at_discharge_vocabulary,meaning_of_visit, origin_of_visit)]

#modification of REPDIM
VISIT_OCCURRENCE <-VISIT_OCCURRENCE[,REPDIM:=substr(REPDIM,1,2)]

# rename variables:
setnames(VISIT_OCCURRENCE, old = "IDUNI", new = "person_id")
setnames(VISIT_OCCURRENCE, old = "DATAMM", new = "visit_start_date")
setnames(VISIT_OCCURRENCE, old = "DATDIM", new = "visit_end_date")
setnames(VISIT_OCCURRENCE, old = "REPDIM", new = "specialty_of_visit")
setnames(VISIT_OCCURRENCE, old = "MODIM", new = "status_at_discharge")
rm(VISIT_OCCURRENCE_)

VISIT_OCCURRENCE<-VISIT_OCCURRENCE[,`:=`(visit_start_date=format(visit_start_date, "%Y%m%d"), visit_end_date=format(visit_end_date, "%Y%m%d"))]

fwrite(VISIT_OCCURRENCE, paste0(diroutput,"/VISIT_OCCURRENCE_SDOTEMP.csv"), quote = "auto")

## Specification table: EVENTS - SDO
# The local tables feeding this CDM table are: SDO, SEA, PS, SALM, RMR. The rules to create EVENTS from each of them are as follows:
  #- SDO:
  # For each record
    # ●	Extract from VISIT_OCCURRENCE (above) the corresponding value of visit_occurrence_id
    # ●	Create a record of EVENTS for each non-empty cell among DIADIM, PAT1,...,PAT5; for each of them, copy the content of the cell in the column code_event, and copy 
    # ●	Copy the values of SDO into EVENTS according to the following table

VISIT_OCCURRENCE_<- SDOTEMP[,visit_occurrence_id:=paste0("SDOTEMP_",seq_along(DATDIM))][,.(IDUNI,DATAMM,DATDIM,DIADIM,PAT1,PAT2,PAT3,PAT4,PAT5,DIADIM_PRES,DIA1_PRES,DIA2_PRES,DIA3_PRES,DIA4_PRES,DIA5_PRES,visit_occurrence_id)]
dim(SDOTEMP); SDOTEMP<-SDOTEMP[,-34]
setnames(VISIT_OCCURRENCE_, old = "DIADIM", new = "PAT0")
setnames(VISIT_OCCURRENCE_, old = "DIADIM_PRES", new = "DIA0_PRES")
#VISIT_OCCURRENCE_<-VISIT_OCCURRENCE_[,N_DIA:= (DIADIM!="")+(PAT1!="")+(PAT2!="")+(PAT3!="")+(PAT4!="")+(PAT5!=""),by=c("IDUNI","visit_occurrence_id")] [,.(IDUNI,DATAMM,DATDIM,DIADIM,PAT1,PAT2,PAT3,PAT4,PAT5,visit_occurrence_id,N_DIA)]
EVENTS_<-melt(VISIT_OCCURRENCE_, measure= patterns("^PAT","^DIA"), value.name = c("event_code", "present_on_admission"), variable.name = "ord") [event_code!="",]
keys<-c("IDUNI","visit_occurrence_id");setkeyv(EVENTS_,keys)

EVENTS<-EVENTS_[present_on_admission==1,present_on_admission:="si"]
EVENTS<-EVENTS_[present_on_admission=="",present_on_admission:="no"]

EVENTS<-EVENTS[,event_record_vocabulary:="ICD9"]
EVENTS<-EVENTS[,`:=`(text_linked_to_event_code="",
  event_free_text="", 
  origin_of_event="administrative_record",
  laterality_of_event="")]
EVENTS<-EVENTS[ord==1,meaning_of_event:="hospitalisation_primary"]
EVENTS<-EVENTS[ord!=1,meaning_of_event:="hospitalisation_secondary"]

# keep only which needed
EVENTS <- EVENTS[,.(IDUNI,DATAMM,DATDIM,event_code,event_record_vocabulary,text_linked_to_event_code,event_free_text,present_on_admission,laterality_of_event,meaning_of_event,origin_of_event,visit_occurrence_id)]

# rename variables:
setnames(EVENTS, old = "IDUNI", new = "person_id")
setnames(EVENTS, old = "DATAMM", new = "start_date_record")
setnames(EVENTS, old = "DATDIM", new = "end_date_record")
rm(VISIT_OCCURRENCE_, EVENTS_)

EVENTS<-EVENTS[,`:=`(start_date_record=format(start_date_record, "%Y%m%d"), end_date_record=format(end_date_record, "%Y%m%d"))]

fwrite(EVENTS, paste0(diroutput,"/EVENTS_SDOTEMP.csv"))

## Specification table: PROCEDURES - SDO
# The local tables feeding this CDM table are: SDO, SPA
  #- SDO:
  # For each record
    # ●	Extract from VISIT_OCCURRENCE (above) the corresponding value of visit_occurrence_id
    # ●	Create a record of EVENTS for each non-empty cell among CODCHI, CODCHI2,...,CODCHI6; for each of them, copy the content of the cell in the column code_event, and copy 
    # ●	Copy the values of SDO into EVENTS according to the following table

VISIT_OCCURRENCE__<- SDOTEMP[,visit_occurrence_id:=paste0("SDOTEMP_",seq_along(DATDIM))] [,.(IDUNI,DATCHI,CODCHI,DATCHI2,CODCHI2,DATCHI3,CODCHI3,DATCHI4,CODCHI4,DATCHI5,CODCHI5, DATCHI6,CODCHI6,visit_occurrence_id)]
#VISIT_OCCURRENCE__<-VISIT_OCCURRENCE__[,N_DIA:= (CODCHI!="")+(CODCHI2!="")+(CODCHI3!="")+(CODCHI4!="")+(CODCHI5!="")+(CODCHI6!=""),by=c("IDUNI","visit_occurrence_id")] [,.(IDUNI,DATCHI,CODCHI,DATCHI2,CODCHI2,DATCHI3,CODCHI3,DATCHI4,CODCHI4,DATCHI5,CODCHI5, DATCHI6,CODCHI6,visit_occurrence_id,N_DIA)]

PROCEDURES_<-melt(VISIT_OCCURRENCE__, measure = patterns("^CODCHI","^DATCHI"), value.name = c("CODCHI", "DATCHI"), variable.name = "ord") 
keys<-c("IDUNI","visit_occurrence_id");setkeyv(PROCEDURES_,keys)

# keep only which needed
PROCEDURES_<-PROCEDURES_[CODCHI!="",]
PROCEDURES<- PROCEDURES_[,`:=`(procedure_code_vocabulary="ICD9PROC",
  meaning_of_procedure="procedure_during_hospitalisation",
  origin_of_procedure="administrative_record")]

# rename variables:
setnames(PROCEDURES, old = "IDUNI", new = "person_id")
setnames(PROCEDURES, old = "DATCHI", new = "procedure_date")
setnames(PROCEDURES, old = "CODCHI", new = "procedure_code")
rm(VISIT_OCCURRENCE__,PROCEDURES_)

PROCEDURES<-PROCEDURES[,.(person_id,procedure_date, procedure_code, procedure_code_vocabulary, visit_occurrence_id, meaning_of_procedure, origin_of_procedure)]

PROCEDURES<-PROCEDURES[,`:=`(procedure_date=format(procedure_date, "%Y%m%d"))]

fwrite(PROCEDURES, paste0(diroutput,"/PROCEDURES_SDOTEMP.csv"))

## Specification table: MEDICAL_OBSERVATIONS- SDO
# The local tables feeding this CDM table are: SDO, AP, SALM
  #- SDO:
  # For each record
    # ●	Extract from VISIT_OCCURRENCE (above) the corresponding value of visit_occurrence_id
    # ●	Create a record of MEDICAL_OBSERVATIONS for each column of this list
      # a.	istruz
      # b.	creatinina
    # ●	Copy the values of SDO into each such record of MEDICAL_OBSERVATIONS according to the following table

VISIT_OCCURRENCE_<- SDOTEMP[,visit_occurrence_id:=paste0("SDOTEMP_",seq_along(DATDIM))] [,.(IDUNI,DATAMM,TITSTU,CREATININA,visit_occurrence_id)]
dim(SDOTEMP); SDOTEMP<-SDOTEMP[,-34]

MEDICAL_OBSERVATIONS_<-melt(VISIT_OCCURRENCE_,measure = c("TITSTU","CREATININA"),value.name = "mo_source_value", variable.name = "mo_source_table") 
keys<-c("IDUNI","visit_occurrence_id");setkeyv(MEDICAL_OBSERVATIONS_,keys)

# 30/09: change format of float variable
MEDICAL_OBSERVATIONS_<-MEDICAL_OBSERVATIONS_[,mo_source_value:=gsub(",",".",mo_source_value)]

# keep only which needed
MEDICAL_OBSERVATIONS_<-MEDICAL_OBSERVATIONS_[mo_source_value!="",]
MEDICAL_OBSERVATIONS_<- MEDICAL_OBSERVATIONS_[,`:=`(mo_source_column="SDO",
  mo_code="",
  mo_record_vocabulary="",
  mo_meaning="measure_during_hospitalisation",
  mo_origin="administrative_record")]
MEDICAL_OBSERVATIONS<-MEDICAL_OBSERVATIONS_[mo_source_table=="CREATININA",mo_unit:="mg/dL"]

# rename variables:
setnames(MEDICAL_OBSERVATIONS, old = "IDUNI", new = "person_id")
setnames(MEDICAL_OBSERVATIONS, old = "DATAMM", new = "mo_date")
rm(VISIT_OCCURRENCE_,MEDICAL_OBSERVATIONS_)

MEDICAL_OBSERVATIONS<-MEDICAL_OBSERVATIONS[,.(person_id,mo_date,mo_code,mo_record_vocabulary, mo_source_table,  mo_source_column, mo_source_value,mo_unit,mo_meaning, mo_origin,visit_occurrence_id)]

MEDICAL_OBSERVATIONS<-MEDICAL_OBSERVATIONS[,`:=`(mo_date=format(mo_date, "%Y%m%d"))]

fwrite(MEDICAL_OBSERVATIONS, paste0(diroutput,"/MEDICAL_OBSERVATIONS_SDOTEMP.csv"))


rm(SDOTEMP)


# 2.  Use SPA  to populate VISIT_OCCURRENCE, then PROCEDURES --------------

for (source in spa){
  pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
  assign(source,pippo)
  setkeyv(pippo,"IDUNI")
 }
rm(pippo)

## Specification table VISIT_OCCURRENCES - SPA : 

  #-SPA
    # For each record with CODPRES=’89.01’ or ‘89.7’ or ‘95.02’ or ‘95.01’
    # ●	Create a record of VISIT_OCCURRENCE and number the records with a sequential number stored in visit_occurrence_id
    # ●	Copy the values of SPA into VISIT_OCCURRENCE according to the following table

for (source in spa){
  pippo <- as.data.table(read_dta(paste0(thisdir,"/input_corrected/",source,".dta")))
  assign(source,pippo)
  setkeyv(pippo,"IDUNI")
  
  pluto<-get(source)
  pippo <- pluto[CODPRES=="89.01"|CODPRES=="89.7"|CODPRES=="95.02"|CODPRES=="95.012",][,visit_occurrence_id:=paste0("SPA",substr(source,4,7),"_",seq_along(DATAINI))]
  
  pippo<-pippo[,`:=`(visit_end_date="",
    specialty_of_visit_vocabulary="SPA.SPECIALI",
    status_at_discharge="",
    status_at_discharge_vocabulary="",
    meaning_of_visit="oupatient_specialist_visit",
    origin_of_visit="SPA")]
  
  setnames(pippo, old="IDUNI",new="person_id")
  setnames(pippo, old="DATAINI",new="visit_start_date")
  setnames(pippo, old="SPECIALI",new="specialty_of_visit")
  
  pippo<-pippo[,.(person_id,visit_occurrence_id,visit_start_date,visit_end_date,specialty_of_visit, specialty_of_visit_vocabulary,status_at_discharge,status_at_discharge_vocabulary,meaning_of_visit,origin_of_visit)]
  
  pippo<-pippo[,`:=`(visit_start_date=format(visit_start_date, "%Y%m%d"))]
  
  assign(paste0("VISIT_OCCURRENCE_",substr(source,4,7)),pippo)
  setkeyv(pippo,"person_id")
  
  fwrite(pippo, paste0(diroutput,"/",paste0("VISIT_OCCURRENCE_",substr(source,4,7)),"_SPA.csv"), quote = "auto")
  
}
rm(pippo, pluto)


## Specification table PROCEDURES - SPA: 
  #- SPA
    # For each record having CODPRES not in the following list:’89.01’or‘89.7 or‘95.02’or‘95.01’
    # ●	Create a record of PROCEDURES 
    # ●	Copy the values of SPA into PROCEDURES according to the following table

for (source in spa){
  pluto<-get(source)
  pippo <- pluto[CODPRES!="89.01"|CODPRES!="89.7"|CODPRES!="95.02"|CODPRES!="95.012",]#[,visit_occurrence_id:=seq_along(DATAINI),by="IDUNI"]
  
  pippo<-pippo[,`:=`(procedure_code_vocabulary="ITA_procedures_coding_system",
    visit_occurrence_id=paste0("SPA",substr(source,4,7),"_",seq_along(DATAINI)),
    meaning_of_procedure="italian_outpatient",
    origin_of_procedure="SPA")]
  
  setnames(pippo, old="IDUNI",new="person_id")
  setnames(pippo, old="DATAINI",new="procedure_date")
  setnames(pippo, old="CODPRES",new="procedure_code")
  
  pippo<-pippo[,.(person_id,procedure_date,procedure_code,procedure_code_vocabulary,visit_occurrence_id,meaning_of_procedure,origin_of_procedure)]
  
  pippo<-pippo[,`:=`(procedure_date=format(procedure_date, "%Y%m%d"))]
  
  assign(paste0("PROCEDURES_",substr(source,4,7)),pippo)
  setkeyv(pippo,"person_id")
  
  fwrite(pippo, paste0(diroutput,"/",paste0("PROCEDURES_",substr(source,4,7)),"_SPA.csv"), quote = "auto")
  
}
rm(pippo, pluto)

rm(list=ls(pattern="^SPA"))

# 3.  Use PS  to populate EVENTS ------------------------------------------

PS <- as.data.table(read_dta(paste0(dirinput,"/PS.dta")))
setkeyv(PS,"IDUNI")

## Specification table EVENTS - PS: 
  # PS:
    # For each record
    # ●	Create one record of EVENTS where the variable event_code is filled with diagnosi_princ; then for those records of PS whose ‘prognosi_testo’ is non empty, create a second record and use prognosi_testo to fill event_free_text    
    # ●	Copy the values of PS into EVENTS according to the following table

EVENTS_PS_<-PS[,prognosi_testo:="xxxx x x, x"][DIAGNOSI_PRINC!="",][,freq:=2]
EVENTS_PS_<-EVENTS_PS_[rep(seq(.N), freq), !"freq"]
KEy<-c("IDUNI","DATA_ORA_ACCETTAZ","DATA_ORA_DIMI"); setkeyv(EVENTS_PS_,KEy)
EVENTS_PS_<-EVENTS_PS_[,rep:=1:.N,by=c("IDUNI","DATA_ORA_ACCETTAZ","DATA_ORA_DIMI")]
EVENTS_PS_<-EVENTS_PS_[rep==1,`:=` (prognosi_testo="",event_record_vocabulary="ICD9",meaning_of_event=	"emergency_room_diagnosis",present_on_admission="")]
EVENTS_PS_<-EVENTS_PS_[rep==2,`:=` (DIAGNOSI_PRINC="",event_record_vocabulary="",meaning_of_event=	"emergency_room_presentation",present_on_admission="yes")]
EVENTS_PS<-EVENTS_PS_[,`:=`(text_linked_to_event_code="", origin_of_event="PS", laterality_of_event="")]
EVENTS_PS<-EVENTS_PS[,visit_occurrence_id:=paste0("PS_",seq_along(DATA_ORA_ACCETTAZ))]

# rename variables:
setnames(EVENTS_PS, old = "IDUNI", new = "person_id")
setnames(EVENTS_PS, old = "DATA_ORA_ACCETTAZ", new = "start_date_record")
setnames(EVENTS_PS, old = "DATA_ORA_DIMI", new = "end_date_record")
setnames(EVENTS_PS, old = "DIAGNOSI_PRINC", new = "event_code")
setnames(EVENTS_PS, old = "prognosi_testo", new = "event_free_text")
rm(EVENTS_PS_)

# keep only needed variables
EVENTS_PS<-EVENTS_PS[,.(person_id,start_date_record,end_date_record,event_code,event_record_vocabulary,text_linked_to_event_code,event_free_text,present_on_admission,laterality_of_event,meaning_of_event,origin_of_event, visit_occurrence_id)]

EVENTS_PS<-EVENTS_PS[,`:=`(start_date_record=format(start_date_record, "%Y%m%d"), end_date_record=format(start_date_record,"%Y%m%d"))]

fwrite(EVENTS_PS, paste0(diroutput,"/EVENTS_PS.csv"), quote = "auto")

rm(PS)

# 4.  Use SEA to populate EVENTS ------------------------------------------

SEA <- as.data.table(read_dta(paste0(dirinput,"/EXE.dta")))
setkeyv(SEA,"IDUNI")

## Specification table EVENTS - SEA: 
  #- SEA
    # For each record
    # ●	Create a record of EVENTS 
    # ●	Copy the values of SEA into EVENTS according to the following table

EVENTS_SEA<-SEA[,visit_occurrence_id:=paste0("SEA_",seq_along(RILASCIO))]
EVENTS_SEA<-EVENTS_SEA[,`:=`(end_date_record="",
                             event_record_vocabulary="ICD9",
                             text_linked_to_event_code="",
                             event_free_text="",
                             present_on_admission="",
                             laterality_of_event="",
                             meaning_of_event="exemption",
                             origin_of_event="SEA")]

# rename variables:
setnames(EVENTS_SEA, old = "IDUNI", new = "person_id")
setnames(EVENTS_SEA, old = "RILASCIO", new = "start_date_record")
setnames(EVENTS_SEA, old = "ICD9CM", new = "event_code")

# keep only needed variables
EVENTS_SEA<-EVENTS_SEA[,.(person_id,start_date_record,end_date_record,event_code,event_record_vocabulary,text_linked_to_event_code,event_free_text,present_on_admission,laterality_of_event,meaning_of_event,origin_of_event,visit_occurrence_id)]

#data transformtion:
EVENTS_SEA<-EVENTS_SEA[,`:=`(start_date_record=format(start_date_record, "%Y%m%d"))]

fwrite(EVENTS_SEA, paste0(diroutput,"/EVENTS_SEA.csv"), quote = "auto")

rm(SEA)

# 5.  Use SALM to populate EVENTS and MEDICAL_OBSERVATIONS  # solo EVENTS  ---------------

SALM <- as.data.table(read_dta(paste0(dirinput,"/SALM.dta")))
setkeyv(SALM,"IDUNI")


##Specification table EVENTS -SALM:
  #- SALM: For each record
    # Create one record of EVENTS for each non-empty value of DIAGNOSI_PRINCIPALE, COMORBIDITA_1, COMORBIDITA_2
    # Copy the values of SALM into EVENTS according to the following table

EVENTS_SALM_<-SALM[DIAGNOSI_PRINCIPALE!=""|COMORBIDITA_1!="" | COMORBIDITA_2!="",][,visit_occurrence_id:=paste0("SALM_",seq_along(DATA))] 
EVENTS_SALM_<-EVENTS_SALM_[SETTORE=="1",event_record_vocabulary:="ICD9"]
EVENTS_SALM_<-EVENTS_SALM_[SETTORE=="2",event_record_vocabulary:="ICD10"]
EVENTS_SALM_<-EVENTS_SALM_[,.(IDUNI,DATA,DIAGNOSI_PRINCIPALE, COMORBIDITA_1,COMORBIDITA_2,event_record_vocabulary,visit_occurrence_id)]

EVENTS_SALM<-melt(EVENTS_SALM_, measure= c("DIAGNOSI_PRINCIPALE","COMORBIDITA_1","COMORBIDITA_2"), variable.name = "ord", value.name = "event_code")[event_code!="",]

EVENTS_SALM<-EVENTS_SALM[,`:=`(end_date_record="",text_linked_to_event_code="",event_free_text="",present_on_admission="yes",origin_of_event="SALM", laterality_of_event="")]
EVENTS_SALM<-EVENTS_SALM[ord=="DIAGNOSI_PRINCIPALE",meaning_of_event:="access_to_mental_health_service_primary"]
EVENTS_SALM<-EVENTS_SALM[ord!="DIAGNOSI_PRINCIPALE",meaning_of_event:="access_to_mental_health_service_comorbidity"]

#30/09 modification: drop 0 or 00 before ICD10 code
EVENTS_SALM<-EVENTS_SALM[event_record_vocabulary=="ICD10" & nchar(event_code)>2, event_code:=gsub("^00","",event_code)]
EVENTS_SALM<-EVENTS_SALM[event_record_vocabulary=="ICD10" & nchar(event_code)>1, event_code:=gsub("^0","",event_code)]
  
# rename variables:
setnames(EVENTS_SALM, old = "IDUNI", new = "person_id")
setnames(EVENTS_SALM, old = "DATA", new = "start_date_record")

EVENTS_SALM<-EVENTS_SALM[,.(person_id,start_date_record,end_date_record,event_code, event_record_vocabulary,text_linked_to_event_code,event_free_text,present_on_admission,laterality_of_event,meaning_of_event,origin_of_event,visit_occurrence_id)]

EVENTS_SALM<-EVENTS_SALM[,`:=`(start_date_record=format(start_date_record, "%Y%m%d"))]

fwrite(EVENTS_SALM, paste0(diroutput,"/EVENTS_SALM.csv"), quote = "auto")
rm(EVENTS_SALM_)

## Specification table MEDICAL_OBSERVATIONS -SALM:
  #- SALM:

rm(SALM)

# ** 6.  Use FED to populate MEDICINES  --------------------------------------------

FED <- as.data.table(read_dta(paste0(dirinput,"/FED.dta")))
setkeyv(FED,"IDUNI")

## Specification table MEDICINES - FED:
  #-FED: For each record
    # ●	Create a record of MEDICINES 
    # ●	Copy the values of FED into MEDICINES according to the following table

## CHANGED FROM PREVIUOS VERSION --> to be updated!!
MEDICINES_<-FED[,visit_occurrence_id:=paste0("FED_",seq_along(DATAERO))][,.(IDUNI,DATAERO,CODFARM,COD_ATC5,visit_occurrence_id)] 

# rename variables:
setnames(MEDICINES_, old = "IDUNI", new = "person_id")
setnames(MEDICINES_, old = "DATAERO", new = "date_dispensing")
setnames(MEDICINES_, old = "CODFARM", new = "medicinal_product_id")
setnames(MEDICINES_, old = "COD_ATC5", new = "medicinal_product_atc_code")

MEDICINES<-MEDICINES_[,`:=`(date_prescription="",disp_number_medicinal_product="",presc_quantity_per_day="",presc_quantity_unit="",presc_duration_days="",product_lot_number="", indication_code="",indication_code_vocabulary="",meaning_of_drug_record="dispensing_in_hospital_pharmacy_unspecified",origin_of_drug_record="FED",prescriber_speciality="",prescriber_speciality_vocabulary="")]

# keep only needed variables
MEDICINES<-MEDICINES[,.(person_id,medicinal_product_id,medicinal_product_atc_code,date_dispensing,date_prescription,disp_number_medicinal_product,presc_quantity_per_day,presc_quantity_unit,presc_duration_days,product_lot_number, indication_code,indication_code_vocabulary,meaning_of_drug_record,origin_of_drug_record,prescriber_speciality,prescriber_speciality_vocabulary,visit_occurrence_id)] 

#data transformtion:
MEDICINES<-MEDICINES[,`:=`(date_dispensing=format(date_dispensing, "%Y%m%d"))]
rm(MEDICINES_)

fwrite(MEDICINES, paste0(diroutput,"/MEDICINES_FED.csv"), quote = "auto")

rm(FED)

# ** 6a. Use FEDTEMP to populate MEDICINES ---------------------------

# for (source in fed){
#   pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
#   assign(source,pippo)
#   setkeyv(pippo,"IDUNI")
# }
# rm(pippo)
FEDTEMP <- as.data.table(read_dta(paste0(dirinput,"/FEDTEMP.dta")))
setkeyv(FED,"IDUNI")


## Specification table MEDICINES - FEDTEMP
  #-FEDTEMP: For each record
    # ●	Create a record of MEDICINES 
    # ●	Copy the values of FED into MEDICINES according to the following table

## CHANGED FROM PREVIUOS VERSION --> to be updated!!
MEDICINES_<-FEDTEMP[,visit_occurrence_id:=paste0("FED_",seq_along(DATAERO))][,.(IDUNI,DATAERO,CODFARM,COD_ATC5,visit_occurrence_id)] 

# rename variables:
setnames(MEDICINES_, old = "IDUNI", new = "person_id")
setnames(MEDICINES_, old = "DATAERO", new = "date_dispensing")
setnames(MEDICINES_, old = "CODFARM", new = "medicinal_product_id")
setnames(MEDICINES_, old = "COD_ATC5", new = "medicinal_product_atc_code")

MEDICINES<-MEDICINES_[,`:=`(date_prescription="",disp_number_medicinal_product="",presc_quantity_per_day="",presc_quantity_unit="",presc_duration_days="",product_lot_number="", indication_code="",indication_code_vocabulary="",meaning_of_drug_record="dispensing_in_hospital_pharmacy_unspecified",origin_of_drug_record="FEDTEMP",prescriber_speciality="",prescriber_speciality_vocabulary="")]

# keep only needed variables
MEDICINES<-MEDICINES[,.(person_id,medicinal_product_id,medicinal_product_atc_code,date_dispensing,date_prescription,disp_number_medicinal_product,presc_quantity_per_day,presc_quantity_unit,presc_duration_days,product_lot_number, indication_code,indication_code_vocabulary,meaning_of_drug_record,origin_of_drug_record,prescriber_speciality,prescriber_speciality_vocabulary,visit_occurrence_id)] 

#data transformtion:
MEDICINES<-MEDICINES[,`:=`(date_dispensing=format(date_dispensing, "%Y%m%d"))]
rm(MEDICINES_)

fwrite(MEDICINES, paste0(diroutput,"/MEDICINES_FED.csv"), quote = "auto")

rm(FED)


rm(list=ls(pattern="^FED"))



### the same procedure with FEDTEMP
FEDTEMP <- as.data.table(read_dta(paste0(thisdir,"/input_corrected/FEDTEMP.dta")))
setkeyv(FEDTEMP,"IDUNI")

##MEDICINES
#-FED: For each record
# ●	Create a record of MEDICINES 
# ●	Copy the values of FED into MEDICINES according to the following table

MEDICINES_<-FEDTEMP[,visit_occurrence_id:=paste0("FED_",seq_along(DATAERO))][,.(IDUNI,DATAERO,PEZZI_ARSNEW,CODFARM,MODERO,COD_ATC5,TARGATURA,visit_occurrence_id)] 

MEDICINES_<-MEDICINES_[,`:=`(date_prescription="",disp_amount_drug_unit="package",presc_units_per_day="",presc_duration="",code_indication="",code_indication_vocabulary="",
  meaning_of_drug_record="dispensing_in_community_pharmacy",origin_of_drug_record="hospital_pharmacy_reimbursement",prescriber_type="")]
MEDICINES_<-MEDICINES_[MODERO=="07",meaning_of_drug_record:="dispensing_in_community_pharmacy"]
MEDICINES_<-MEDICINES_[(MODERO=="10" | MODERO=="11"),meaning_of_drug_record:="dispensing_in_hospital_pharmacy_for_oupatient_use"]
MEDICINES_<-MEDICINES_[MODERO=="06",meaning_of_drug_record:="dispensing_in_hospital_pharmacy_for_inpatient_use"]
MEDICINES_<-MEDICINES_[(MODERO!="10" | MODERO!="11" |MODERO!="06" |MODERO!="07"),meaning_of_drug_record:="dispensing_in_hospital_pharmacy_for_home_use"]

# rename variables:
setnames(MEDICINES_, old = "IDUNI", new = "person_id")
setnames(MEDICINES_, old = "DATAERO", new = "date_dispensing")
setnames(MEDICINES_, old = "PEZZI_ARSNEW", new = "disp_amount_drug")
setnames(MEDICINES_, old = "CODFARM", new = "product_code")
setnames(MEDICINES_, old = "COD_ATC5", new = "product_ATCcode")
setnames(MEDICINES_, old = "TARGATURA", new = "product_lot_number")

# keep only needed variables
MEDICINES<-MEDICINES_[,.(person_id,date_dispensing,date_prescription,disp_amount_drug,disp_amount_drug_unit,presc_units_per_day,presc_duration,product_code,product_ATCcode,code_indication,code_indication_vocabulary,meaning_of_drug_record,origin_of_drug_record,prescriber_type,visit_occurrence_id)] #product_lot_number,

#data transformtion:
MEDICINES<-MEDICINES[,`:=`(date_dispensing=format(date_dispensing, "%Y%m%d"))]
rm(MEDICINES_)

fwrite(MEDICINES, paste0(diroutput,"/MEDICINES_FEDTEMP.csv"),)

rm(FEDTEMP)

# ** 7.  Use SPF to populate MEDICINES --------------------------------------------

for (source in spf){
  pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
  assign(source,pippo)
  setkeyv(pippo,"IDUNI")
}
rm(pippo)

## CHANGED FROM PREVIUOS VERSION --> to be updated!!

for (source in spf){
  pippo<-get(source)
  
  pippo<-pippo[,`:=`(visit_occurrence_id=paste0("SPF",substr(source,4,7),"_",seq_along(DATAERO)))][,.(IDUNI,DATAERO,CODFARM,COD_ATC5,visit_occurrence_id)]
  pippo<-pippo[,`:=`(date_prescription="",disp_number_medicinal_product="",presc_quantity_per_day="",presc_quantity_unit="",presc_duration_days="",product_lot_number="", indication_code="",indication_code_vocabulary="",meaning_of_drug_record="dispensing_in_community_pharmacy",origin_of_drug_record="FED",prescriber_speciality="",prescriber_speciality_vocabulary="")]
                
  
  setnames(pippo, old="IDUNI",new="person_id")
  setnames(pippo, old="DATAERO",new="date_dispensing")
  setnames(pippo, old="CODFARM",new="medicinal_product_id")
  setnames(pippo, old ="COD_ATC5",new = "medicinal_product_atc_code")
  
  pippo<-pippo[,.(person_id,medicinal_product_id,medicinal_product_atc_code,date_dispensing,date_prescription,disp_number_medicinal_product,presc_quantity_per_day,presc_quantity_unit,presc_duration_days,product_lot_number, indication_code,indication_code_vocabulary,meaning_of_drug_record,origin_of_drug_record,prescriber_speciality,prescriber_speciality_vocabulary,visit_occurrence_id)]
  
  pippo<-pippo[,`:=`(date_dispensing=format(date_dispensing, "%Y%m%d"))]
  
  assign(paste0("MEDICINES_",substr(source,4,7)),pippo)
  setkeyv(pippo,"person_id")
  
  fwrite(pippo, paste0(diroutput,"/",paste0("MEDICINES_",substr(source,4,7)),"_SPF.csv"), quote = "auto")

}


rm(pippo)

rm(list=ls(pattern="^SPF"))
rm(list=ls(pattern="^MEDICINES"))


# 8.  Use CAP to populate SURVEY_ID, then SURVEY_OBSERVATIONS -------------

## Specification table SURVEY_ID - CAP1: 
# The local tables feeding this CDM table are: CAP, ABS, IVG, RMR. The rules to create SURVEY_ID from them are as follows

#- CAP: For each record
  # ●	Create one record of SURVEY_ID for the mother, and as many records as the number of children and number the records with a sequential number stored in survey_id
  # ●	Copy the values of CAP into SURVEY_ID according to the following table for each maternal record seq(1,uniqueN(IDUNI))
for (source in cap){
  pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
  assign(source,pippo)
  setkeyv(pippo,"IDUNI")
}
rm(pippo)

## 12/04:added to remove empty IDUNI
CAP1<-CAP1[IDUNI!="",]
CAP2<-CAP2[IDUNI!="",]
CAP1<-CAP1[!is.na(DATAPARTO_ARSNEW),]
CAP2<-CAP2[DATAPARTO_ARSNEW!="",]

CAP1<-CAP1[,DATAPARTO_ARSNEW:=as.Date(DATAPARTO_ARSNEW)]
CAP2<-CAP2[,DATAPARTO_ARSNEW:=as.Date(DATAPARTO_ARSNEW)]

# #12/04: verify unique pregnancy-row
# CAP01<-unique(CAP1, by=c("IDUNI","DATAPARTO_ARSNEW","ID_CAP1_ARSNEW"))
# CAP02<-unique(CAP2, by=c("IDUNI","DATAPARTO_ARSNEW","ID_CAP1_ARSNEW"))

# 14/04: change survey_id per twins
CAP1<-CAP1[,id:=.GRP, by = IDUNI]
setDT(CAP1)[,survey_id :=paste0("CAP_",id)][,-"id"]
# 14/04: add unique
CAP01<-unique(CAP1[,.(IDUNI,survey_id,DATAPARTO_ARSNEW)])
SURVEY_ID<-CAP01[,`:=`(survey_meaning="birth_registry_mother",
                       survey_origin="CAP1")]
setnames(SURVEY_ID, old="IDUNI", new = "person_id")
setnames(SURVEY_ID, old="DATAPARTO_ARSNEW", new = "survey_date")
#rm(CAP10)

## Specification table SURVEY_ID - CAP2: 
# The local tables feeding this CDM table are: CAP, ABS, IVG, RMR. The rules to create SURVEY_ID from them are as follows

#- CAP: For each record
# ●	Create one record of SURVEY_ID for the mother, and as many records as the number of children and number the records with a sequential number stored in survey_id
# ●	Copy the values of CAP into SURVEY_ID according to the following table for each maternal record seq(1,uniqueN(IDUNI))

CAP20<-merge(CAP2,CAP1[,.(ID_CAP1_ARSNEW,survey_id)],by="ID_CAP1_ARSNEW")
#14/04: add unique
CAP202<-unique(CAP20[,.(IDUNI,survey_id,DATAPARTO_ARSNEW)])
SURVEY_ID2<-CAP202[,`:=`(survey_meaning="birth_registry_mother",
                         survey_origin="CAP2")]
setnames(SURVEY_ID2, old="IDUNI", new = "person_id")
setnames(SURVEY_ID2, old="DATAPARTO_ARSNEW", new = "survey_date")
#rm(CAP20)

SURVEY_ID<-SURVEY_ID[,`:=`(survey_date=format(survey_date, "%Y%m%d"))]
SURVEY_ID2<-SURVEY_ID2[,`:=`(survey_date=format(survey_date, "%Y%m%d"))]

# 21/04: verify unique person_survey
SURV<-rbind(SURVEY_ID[,.(person_id,survey_id)],SURVEY_ID2[,.(person_id,survey_id)])
dup_idx <- duplicated(SURV)
SURV_rows <- SURV[dup_idx, survey_id]

SURVEY_ID <- SURVEY_ID[ ! SURVEY_ID$survey_id %in% SURV_rows, ]
SURVEY_ID2 <- SURVEY_ID2[ ! SURVEY_ID2$survey_id %in% SURV_rows, ]

CAP1 <- CAP1[ ! CAP1$survey_id %in% SURV_rows, ]
CAP2<- CAP2[ ! CAP2$survey_id %in% SURV_rows, ]

fwrite(SURVEY_ID, paste0(diroutput,"SURVEY_ID_CAP1.csv"), quote = "auto")
fwrite(SURVEY_ID2, paste0(diroutput,"SURVEY_ID_CAP2.csv"), quote = "auto")

## Specification table SURVEY_OBSERVATIONS - CAP1: 
#The local tables feeding this CDM table are: CAP, ABS, IVG, RMR

  #-CAP:For each record of the mother
    # ●	Extract from SURVEY_ID (above) the corresponding value of survey_id
    # ●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below; 

#SURVEY_OBSERVATIONS_0<-CAP1[,survey_id:=seq_along(IDUNI)][,.(IDUNI,DATAPARTO_ARSNEW, survey_id)]
#names(CAP1)
CAP1<-CAP1[,DATAPARTO_ARSNEW:=as.character(DATAPARTO_ARSNEW)]

vars<-c("ABORTI","ACCR_FET","ALTEZZA","AMNIO","ANON_M","CESAREO_ARSNEW","CITTU_ARSNEW_M","CITTU_ARSNEW_P","CONCEP","CONDPR_M","CONDPR_P","CONSANG","DATNAS_P","DEC_GRAV","DOVE_GRAV","DOWN","ECO_22","ETA_M_ARSNEW","ETA_P_ARSNEW","FETOSCOP","FUMO","GENERE","GEST_ECO","IVG","METODO","NATIMORTI","NATIVIVI","NRECOGR","NRECOGR","NRFEM","NRIND","NRMASCHI","PARTI_ARSNEW","PESO_PRE","POSPR_M","POSPR_P","PRIMA_VI","RAMATT_M","RAMATT_P","RH","RIPRASS","SETTAMEN_ARSNEW","STATCIV_M","STATONAS_M_ARSNEW","STATONAS_P_ARSNEW","TITSTU_M_ARSNEW","TITSTU_P_ARSNEW","VILLI_C","VISITE_ARSNEW","DATAPARTO_ARSNEW") #"DATAPARTO_ARSNEW" added
CAP10<-CAP1[ABORTI!="" | ACCR_FET!="" | ALTEZZA!="" | AMNIO!="" | ANON_M!="" | CESAREO_ARSNEW!="" | CITTU_ARSNEW_M!="" | CITTU_ARSNEW_P!="" | CONCEP!="" | CONDPR_M!="" | CONDPR_P!="" | CONSANG!="" | !is.na(DATNAS_P) | DEC_GRAV!="" | DOVE_GRAV!="" | DOWN!="" | ECO_22!="" | ETA_M_ARSNEW!="" | ETA_P_ARSNEW!="" | FETOSCOP!="" | FUMO!="" | GENERE!="" | GEST_ECO!="" | IVG!="" | METODO!="" | NATIMORTI!="" | NATIVIVI!="" | NRECOGR!="" | NRECOGR!="" | NRFEM!="" | NRIND!="" | NRMASCHI!="" | PARTI_ARSNEW!="" | PESO_PRE!="" | POSPR_M!="" | POSPR_P!="" | PRIMA_VI!="" | RAMATT_M!="" | RAMATT_P!="" | RH!="" | RIPRASS!="" | SETTAMEN_ARSNEW!="" | STATCIV_M!="" | STATONAS_M_ARSNEW!="" | STATONAS_P_ARSNEW!="" | TITSTU_M_ARSNEW!="" | TITSTU_P_ARSNEW!="" | VILLI_C!="" | VISITE_ARSNEW!=""| !is.na(DATAPARTO_ARSNEW),]

#14/04: add unique
CAP101<-unique(CAP10,by=c("IDUNI","survey_id","DATAPARTO_ARSNEW"))

CAP11<-melt(CAP101, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value") 
#SURVEY_OBSERVATIONS_CAP1<-CAP11[,survey_id:=paste0("SO_CAP1_",seq_along(IDUNI))]
SURVEY_OBSERVATIONS_CAP1<-CAP11[,`:=`(so_source_table="CAP1",so_origin="CAP1", so_meaning="birth_registry_mother")]
SURVEY_OBSERVATIONS_CAP1<-SURVEY_OBSERVATIONS_CAP1[so_source_column=="PESO_PRE",so_unit:="kg"]


## duplicate DATAPARTO_ARSNEW, as it is and in "so_date"
SURVEY_OBSERVATIONS_CAP1<-merge(SURVEY_OBSERVATIONS_CAP1,CAP1[,.(IDUNI,ID_CAP1_ARSNEW,DATAPARTO_ARSNEW)],by=c("IDUNI","ID_CAP1_ARSNEW"),all.x = T)

setnames(SURVEY_OBSERVATIONS_CAP1, old="IDUNI", new="person_id")
setnames(SURVEY_OBSERVATIONS_CAP1, old="DATAPARTO_ARSNEW", new="so_date")
SURVEY_OBSERVATIONS_CAP1[,`:=`(so_date=as.Date(so_date))]
SURVEY_OBSERVATIONS_CAP1[,`:=`(so_date=format(so_date, "%Y%m%d"))]
SURVEY_OBSERVATIONS_CAP1[,.(person_id,so_date,so_source_table,so_source_column, so_source_value,so_unit,so_origin,so_meaning,survey_id)]

fwrite(SURVEY_OBSERVATIONS_CAP1, paste0(diroutput,"/SURVEY_OBSERVATIONS_CAP1.csv"), quote = "auto")
rm(CAP10, CAP11, CAP01)

## Specification table SURVEY_OBSERVATIONS - CAP2: 
#The local tables feeding this CDM table are: CAP, ABS, IVG, RMR

  #-CAP2: For each record of each child
    # ●	Extract from SURVEY_ID (above) the corresponding value of survey_id
    # ●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below

CAP20<-CAP20[,DATAPARTO_ARSNEW:=as.character(DATAPARTO_ARSNEW)]

vars<-c("ALLATTA","ALTROSAN","ANESTES","APGAR","CARIOTIPO","CIRCOST","CIRC_CRA","DESMAL_1","DESMAL_2","D_CICOST","D_MAL_1M","D_MAL_2M","D_MAL_GR1","D_MAL_GR2","ES_STRUM","ES_STRUM","ETA_G_MAL","ETA_NEO","FARM_TRAV","FOTOGRAF","GENITALI","GINECOL","INDIFARM_ARSNEW","INDOTTO_ARSNEW","INTUBAZ","KRISTELLER","LUNGH","LUOGO","MALFOR","MALF_1","MALF_2","MALF_3","MALF_FRA","MALF_G_M","MALF_G_P","MALF_M","MALF_P","MALF_P_M","MALF_P_P","MAL_1","MAL_1_M","MAL_2","MAL_2_M","MAL_GR_1","MAL_GR_2","MOD_PART","MON_MOR","OSTETRIC","PEDIAT","PESO","PRESENZA","PRES_NEO","PROF_RH","PROF_RHNO","PROGFIGL","RIANIMAZ","RISAUT","SESSO","TRAVAGLIO","VENTILAZ","VITALITA_ARSNEW","DATAPARTO_ARSNEW") #"DATAPARTO_ARSNEW" added
CAP200<-CAP20[ALLATTA!="" | ALTROSAN!="" | ANESTES!="" | APGAR!="" | CARIOTIPO!="" | CIRCOST!="" | CIRC_CRA!="" | DESMAL_1!="" | DESMAL_2!="" | D_CICOST!="" | D_MAL_1M!="" | D_MAL_2M!="" | D_MAL_GR1!="" | D_MAL_GR2!="" | ES_STRUM!="" | ES_STRUM!="" | ETA_G_MAL!="" | ETA_NEO!="" | FARM_TRAV!="" | FOTOGRAF!="" | GENITALI!="" | GINECOL!="" | INDIFARM_ARSNEW!="" | INDOTTO_ARSNEW!="" | INTUBAZ!="" | KRISTELLER!="" | LUNGH!="" | LUOGO!="" | MALFOR!="" | MALF_1!="" | MALF_2!="" | MALF_3!="" | MALF_FRA!="" | MALF_G_M!="" | MALF_G_P!="" | MALF_M!="" | MALF_P!="" | MALF_P_M!="" | MALF_P_P!="" | MAL_1!="" | MAL_1_M!="" | MAL_2!="" | MAL_2_M!="" | MAL_GR_1!="" | MAL_GR_2!="" | MOD_PART!="" | MON_MOR!="" | OSTETRIC!="" | PEDIAT!="" | PESO!="" | PRESENZA!="" | PRES_NEO!="" | PROF_RH!="" | PROF_RHNO!="" | PROGFIGL!="" | RIANIMAZ!="" | RISAUT!="" | SESSO!="" | TRAVAGLIO!="" | VENTILAZ!="" | VITALITA_ARSNEW!=""| DATAPARTO_ARSNEW!="",]

#14/04: add unique
CAP201<-unique(CAP200,by=c("IDUNI","survey_id","DATAPARTO_ARSNEW"))

CAP21<-melt(CAP201, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value") 
#SURVEY_OBSERVATIONS_CAP2<-CAP21[,survey_id:=paste0("SO_CAP2_",seq_along(IDUNI))]
SURVEY_OBSERVATIONS_CAP2<-CAP21[,`:=`(so_source_table="CAP2",so_origin="CAP2",so_meaning="birth_registry_child")]
SURVEY_OBSERVATIONS_CAP2<-SURVEY_OBSERVATIONS_CAP2[so_source_column=="PESO",so_unit:="g"][so_source_column=="LUNGHEZZA",so_unit:="cm"]
SURVEY_OBSERVATIONS_CAP2<-SURVEY_OBSERVATIONS_CAP2[so_source_column=="MAL_1" |so_source_column=="MAL_2" | so_source_column=="MAL_1_M" |so_source_column=="MAL_2_M" | so_source_column=="MALF_1"|so_source_column=="MALF_2" |so_source_column=="MALF_3" |so_source_column=="CIRCOST" ,so_unit:="ICD9"]

## duplicate DATAPARTO_ARSNEW, as it is and in "so_date"
SURVEY_OBSERVATIONS_CAP2<-merge(SURVEY_OBSERVATIONS_CAP2,CAP2[,.(IDUNI,ID_CAP1_ARSNEW,DATAPARTO_ARSNEW)],by=c("IDUNI","ID_CAP1_ARSNEW"),all.x = T)

setnames(SURVEY_OBSERVATIONS_CAP2, old="IDUNI", new="person_id")
setnames(SURVEY_OBSERVATIONS_CAP2, old="DATAPARTO_ARSNEW", new="so_date")
SURVEY_OBSERVATIONS_CAP2[,`:=`(so_date=as.Date(so_date))]
SURVEY_OBSERVATIONS_CAP2[,`:=`(so_date=format(so_date, "%Y%m%d"))]

SURVEY_OBSERVATIONS_CAP2<-SURVEY_OBSERVATIONS_CAP2[,.(person_id,so_date,so_source_table,so_source_column,so_source_value,so_unit,so_origin,so_meaning,survey_id)]

fwrite(SURVEY_OBSERVATIONS_CAP2, paste0(diroutput,"/SURVEY_OBSERVATIONS_CAP2.csv"), quote = "auto")
rm(CAP20, CAP21)

rm(list=ls(pattern="^CAP"))

# 9.  Use ABS to populate SURVEY_ID, then SURVEY_OBSERVATIONS -------------

##Specification table SURVEY_ID - ABS:
  #-ABS:For each record
    # ●	Create one record of SURVEY_ID and number the records with a sequential number stored in survey_id
    # ●	Copy the values of ABS into SURVEY_ID according to the following table 

ABS <- as.data.table(read_dta(paste0(dirinput,"/ABS.dta")))
setkeyv(ABS,"IDUNI")

SURVEY_ID_ABS<-ABS[,`:=`(survey_id=paste0("ABS_",seq_along(IDUNI)),
                         survey_meaning="spontaneous_abortion_registry",
                         survey_origin="ABS")]

setnames(SURVEY_ID_ABS,"IDUNI", "person_id")
setnames(SURVEY_ID_ABS,"DATAINT", "survey_date")

SURVEY_ID_ABS<-SURVEY_ID_ABS[,.(person_id,survey_id,survey_date,survey_meaning,survey_origin)]

SURVEY_ID_ABS<-SURVEY_ID_ABS[,`:=`(survey_date=format(survey_date, "%Y%m%d"))]

fwrite(SURVEY_ID_ABS, paste0(diroutput,"/SURVEY_ID_ABS.csv"), quote = "auto")

setnames(ABS,"survey_date","DATAINT")
setnames(ABS, "person_id","IDUNI")

## Specification table SURVEY_OBSERVATION - ABS:
#-ABS: For each record 
    # ●	Extract from SURVEY_ID (above) the corresponding value of survey_id
    # ●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below
ABS<-ABS[,DATAINT:=as.character(DATAINT)]

vars<-c("ABORTI","ANONIMO","CAUSA","CITTU_ARSNEW","COMPLIC","CONDPROF","DEGENZA","ETAMADRE_ARSNEW","FIGLI","IVG","LUOGO","METODO","NATMORTI","NATVIVI","RAMATT","RIPRASS","SETTAMEN_ARSNEW","STATCIV","TERAPIA","TIPO","TITSTU","DATAINT") #DATAINT added
SURVEY_OBSERVATIONS_ABS_<-ABS[ABORTI!="" | ANONIMO!="" | CAUSA!="" | CITTU_ARSNEW!="" | COMPLIC!="" | CONDPROF!="" | DEGENZA!="" | ETAMADRE_ARSNEW!="" | FIGLI!="" | IVG!="" | LUOGO!="" | METODO!="" | NATMORTI!="" | NATVIVI!="" | RAMATT!="" | RIPRASS!="" | SETTAMEN_ARSNEW!="" | STATCIV!="" | TERAPIA!="" | TIPO!="" | TITSTU!="" | !is.na(DATAINT),]

SURVEY_OBSERVATIONS_ABS<-melt(SURVEY_OBSERVATIONS_ABS_, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value") 

SURVEY_OBSERVATIONS_ABS<-SURVEY_OBSERVATIONS_ABS[,`:=`(so_unit="", 
                                                       so_source_table="ABS",
                                                       so_origin="ABS",
                                                       so_meaning="spontaneous_abortion_registry")]
## duplicate DATAINT, as it is and in "so_date"
SURVEY_OBSERVATIONS_ABS<-merge(SURVEY_OBSERVATIONS_ABS,ABS[,.(IDUNI,survey_id,DATAINT)],by=c("IDUNI","survey_id"),all.x = T)

setnames(SURVEY_OBSERVATIONS_ABS,"DATAINT","so_date")
setnames(SURVEY_OBSERVATIONS_ABS,"IDUNI","person_id")

SURVEY_OBSERVATIONS_ABS[,`:=`(so_date=as.Date(so_date))]
SURVEY_OBSERVATIONS_ABS[,`:=`(so_date=format(so_date, "%Y%m%d"))]

SURVEY_OBSERVATIONS_ABS<-SURVEY_OBSERVATIONS_ABS[,.(person_id,so_date,so_source_table,so_source_column,so_source_value,so_unit,so_origin,so_meaning,survey_id)]

fwrite(SURVEY_OBSERVATIONS_ABS, paste0(diroutput,"/SURVEY_OBSERVATIONS_ABS.csv"), quote = "auto")

rm(ABS)

# 10. Use IVG to populate SURVEY_ID, then SURVEY_OBSERVATIONS ------------

##Specification table SURVEY_ID - IVG:
  #- IVG:For each record
    # ●	Create one record of SURVEY_ID and number the records with a sequential number stored in survey_id
    # ●	Copy the values of IVG into SURVEY_ID according to the following table 

IVG <- as.data.table(read_dta(paste0(dirinput,"/IVG.dta")))
setkeyv(IVG,"IDUNI")

SURVEY_ID_IVG<-IVG[,survey_id:=paste0("IVG_",seq_along(IDUNI))]
SURVEY_ID_IVG<-SURVEY_ID_IVG[,.(IDUNI,survey_id, DATAINT)]
SURVEY_ID_IVG<-SURVEY_ID_IVG[,`:=`(survey_meaning="induced_termination_registry",
                                   survey_origin="IVG")]

setnames(SURVEY_ID_IVG, old="IDUNI", new="person_id")
setnames(SURVEY_ID_IVG, old="DATAINT", new="survey_date")

SURVEY_ID_IVG<-SURVEY_ID_IVG[,`:=`(survey_date=format(survey_date, "%Y%m%d"))]

fwrite(SURVEY_ID_IVG, paste0(diroutput,"/SURVEY_ID_IVG.csv"), quote = "auto")

## Specification table SURVEY_OBSERVATION -IVG:
  #-IVG: For each record 
    # ●	Extract from SURVEY_ID (above) the corresponding value of survey_id
    # ●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below
IVG<-IVG[,DATAINT:=as.character(DATAINT)]

vars<-c("CONDPROF","POSPROF","RAMATT","NATVIVI","NATMORTI","FIGLI","CITTU_ARSNEW","MALFOR","DESCR_INTERV","PROSTAGLANDINE","COMPLIC0","COMPLIC1","COMPLIC2","COMPLIC3","COMPLIC4","COMPLIC0_ARSNEW","TITSTU_ARSNEW","STATCIV_ARSNEW","NATVIV_ARSNEW","NATMORT_ARSNEW","ABORTI_ARSNEW","IVG_ARSNEW","SETTAMEN","CERTIF","URGENZA","ASSENSO","LUOGO","TIPO","TERAPIA","DEGENZA","COMPLIC","ETAMADRE_ARSNEW","ETAGEST_ARSNEW","DATAINT") #"DATAINT" added
SURVEY_OBSERVATIONS_IVG_<-IVG[CONDPROF!="" | POSPROF!="" | RAMATT!="" | !is.na(NATVIVI) | !is.na(NATMORTI) | !is.na(FIGLI) | CITTU_ARSNEW!="" | MALFOR!="" | DESCR_INTERV!="" | !is.na(PROSTAGLANDINE) | COMPLIC0!="" | COMPLIC1!="" | COMPLIC2!="" | COMPLIC3!="" | COMPLIC4!="" | COMPLIC0_ARSNEW!="" | TITSTU_ARSNEW!="" | STATCIV_ARSNEW!="" | NATVIV_ARSNEW!="" | NATMORT_ARSNEW!="" | ABORTI_ARSNEW!="" | IVG_ARSNEW!="" | !is.na(SETTAMEN) | CERTIF!="" | URGENZA!="" | ASSENSO!="" | LUOGO!="" | TIPO!="" | TERAPIA!="" | !is.na(DEGENZA) | COMPLIC!="" | !is.na(ETAMADRE_ARSNEW) | ETAGEST_ARSNEW!="" | DATAINT!="",]

SURVEY_OBSERVATIONS_IVG<-melt(SURVEY_OBSERVATIONS_IVG_, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value") 

SURVEY_OBSERVATIONS_IVG<-SURVEY_OBSERVATIONS_IVG[,`:=`(so_unit="",
                                                       so_source_table="IVG",
                                                       so_origin="IVG",
                                                       so_meaning="induced_termination_registry")]
## duplicate DATAINT, as it is and in "so_date"
SURVEY_OBSERVATIONS_IVG<-merge(SURVEY_OBSERVATIONS_IVG,IVG[,.(IDUNI,survey_id,DATAINT)],by=c("IDUNI","survey_id"),all.x = T)

setnames(SURVEY_OBSERVATIONS_IVG,"IDUNI","person_id")
setnames(SURVEY_OBSERVATIONS_IVG,"DATAINT","so_date")

SURVEY_OBSERVATIONS_IVG<-SURVEY_OBSERVATIONS_IVG[,.(person_id,so_date,so_source_table,so_source_column,so_source_value,so_unit,so_origin,so_meaning,survey_id)]

SURVEY_OBSERVATIONS_IVG[,`:=`(so_date=as.Date(so_date))]
SURVEY_OBSERVATIONS_IVG[,`:=`(so_date=format(so_date, "%Y%m%d"))]

fwrite(SURVEY_OBSERVATIONS_IVG, paste0(diroutput,"/SURVEY_OBSERVATIONS_IVG.csv"), quote = "auto")

rm(IVG)

# 11. Use RMR to populate SURVEY_ID, then SURVEY_OBSERVATIONS ------------

## Specification table SURVEY_ID - RMR:
  #- RMR:For each record
    # ●	Create one record of SURVEY_ID and number the records with a sequential number stored in survey_id
    # ●	Copy the values of RMR into SURVEY_ID according to the following table 

RMR <- as.data.table(read_dta(paste0(dirinput,"/RMR.dta")))
setkeyv(RMR,"IDUNI")

SURVEY_ID_RMR<-RMR[,survey_id:=paste0("RMR_",seq_along(IDUNI))]
SURVEY_ID_RMR<-SURVEY_ID_RMR[,.(IDUNI,survey_id,DATMORTE)]
SURVEY_ID_RMR<-SURVEY_ID_RMR[,`:=`(survey_meaning="death_registry",
                                   survey_origin="RMR")]

setnames(SURVEY_ID_RMR, old="IDUNI", new="person_id")
setnames(SURVEY_ID_RMR, old="DATMORTE", new="survey_date")

SURVEY_ID_RMR<-SURVEY_ID_RMR[,`:=`(survey_date=format(survey_date, "%Y%m%d"))]

fwrite(SURVEY_ID_RMR, paste0(diroutput,"/SURVEY_ID_RMR.csv"), quote = "auto")

## Specification tabl SURVEY_OBERSVATION - RMR:
  #-RMR:For each record 
    # ●	Extract from SURVEY_ID (above) the corresponding value of survey_id
    # ●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below
vars<-c("CAUSAMORTE","CAUSAMORTE_ICDX","CAUSAVIOLENTA","CAUSAVIOLENTA_ICDX","NDOC","LUOGO","LUOGOACC","MEV_GD","MEV_TI")
SURVEY_OBSERVATIONS_RMR_<-RMR[CAUSAMORTE!="" | CAUSAMORTE_ICDX!="" | CAUSAVIOLENTA!="" | CAUSAVIOLENTA_ICDX!="" | NDOC!="" | LUOGO!="" | LUOGOACC!="" | MEV_GD!="" | MEV_TI!="",]# !is.na(DATAINT)

SURVEY_OBSERVATIONS_RMR<-melt(SURVEY_OBSERVATIONS_RMR_, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value") 

SURVEY_OBSERVATIONS_RMR<-SURVEY_OBSERVATIONS_RMR[,`:=`(so_source_table="RMR",
                                                       so_origin="RMR",
                                                       so_meaning="death_registry")] #so_unit="",
SURVEY_OBSERVATIONS_RMR<-SURVEY_OBSERVATIONS_RMR[so_source_column=="CAUSAMORTE",so_unit:='ICD9'][so_source_column=="CAUSAMORTE_ICDX",so_unit:='ICD10']
                                                 
setnames(SURVEY_OBSERVATIONS_RMR,"IDUNI","person_id")
setnames(SURVEY_OBSERVATIONS_RMR,"DATMORTE","so_date")

SURVEY_OBSERVATIONS_RMR<-SURVEY_OBSERVATIONS_RMR[,.(person_id,so_date,so_source_table,so_source_column,so_source_value,so_unit,so_origin,so_meaning,survey_id)]

SURVEY_OBSERVATIONS_RMR<-SURVEY_OBSERVATIONS_RMR[,`:=`(so_date=format(so_date, "%Y%m%d"))]

fwrite(SURVEY_OBSERVATIONS_RMR, paste0(diroutput,"/SURVEY_OBSERVATIONS_RMR.csv"), quote = "auto")

rm(RMR)

# 12. Use AP to populate VISIT_OCCURRENCE, then MEDICAL_OBSERVATIONS  ----------------------------

##Specification table VISIT_OCCURRENCE - AP:
  #-AP: For each record 
    # ●	Create a record of VISIT_OCCURRENCE and label the records with a unique code stored in visit_occurrence_id (primary key)
    # ●	Copy the values of AP into VISIT_OCCURRENCE according to the following table

AP <- as.data.table(read_dta(paste0(dirinput,"/AP.dta")))
setkeyv(AP,"IDUNI")

## 2/12: drop 2 first character if they are letters in COD_MORF_1, COD_MORF_2, COD_MORF_3, COD_TOPOG
AP<-AP[COD_MORF_1=="00000000" | COD_MORF_1=="000000000" | COD_MORF_1=="0000", COD_MORF_1:=NA]
AP<-AP[COD_MORF_2=="00000000" | COD_MORF_2=="000000000" | COD_MORF_2=="0000", COD_MORF_2:=NA]
AP<-AP[COD_MORF_3=="00000000" | COD_MORF_3=="000000000" | COD_MORF_3=="0000", COD_MORF_3:=NA]
AP<-AP[COD_TOPOG=="00000000" | COD_TOPOG=="000000000" | COD_TOPOG=="0000", COD_TOPOG:=NA]


VISIT_OCCURRENCE_AP<-AP[,`:=`(visit_occurrence_id=paste0("AP_",seq_along(DAT_ACC)))][,.(IDUNI, DAT_ACC, visit_occurrence_id)]
VISIT_OCCURRENCE_AP<-VISIT_OCCURRENCE_AP[,`:=`(visit_end_date="",specialty_of_visit="",specialty_of_visit_vocabulary="",status_at_discharge="",status_at_discharge_vocabulary="",meaning_of_visit="pathology_report",origin_of_visit="AP")]

setnames(VISIT_OCCURRENCE_AP, "IDUNI", "person_id")
setnames(VISIT_OCCURRENCE_AP, "DAT_ACC", "visit_start_date")

VISIT_OCCURRENCE_AP<-VISIT_OCCURRENCE_AP[,`:=`(visit_start_date=format(visit_start_date, "%Y%m%d"))]

fwrite(VISIT_OCCURRENCE_AP, paste0(diroutput,"/VISIT_OCCURRENCE_AP.csv"), quote = "auto")

## Specification table MEDICAL_OBSERVATIONS - AP:
  #-AP: For each record
    # ●	Extract from VISIT_OCCURRENCE (above) the corresponding value of visit_occurrence_id
    # ●	Create a record of MEDICAL_OBSERVATIONS for each non-empty column of this list
      # a.	Columns containing codes
        # i.	COD_MORF_1, 
        # ii.	COD_MORF_2, 
        # iii.COD_MORF_3,
        # iv.	COD_TOPOG
      # b.	Columns containing text:
        # i.	DIAGNOSI
        # ii.	MACROSCOPIA
    # ●	Copy the values of AP into each such record of MEDICAL_OBSERVATIONS according to the following table 

MEDICAL_OBSERVATIONS_<-AP[(COD_MORF_1!="" | COD_MORF_2!="" | COD_MORF_3!="" | COD_TOPOG!="") | (DIAGNOSI!="" | MACROSCOPIA!="") ,]

MEDICAL_OBSERVATIONS_W<-melt(MEDICAL_OBSERVATIONS_, measure.vars = c("COD_MORF_1","COD_MORF_2","COD_MORF_3","COD_TOPOG", "MACROSCOPIA","DIAGNOSI"), variable.name = "mo_source_column", value.name = "mo_source_value")[mo_source_value!="",]
#keys<-c("IDUNI","visit_occurrence_id");setkeyv(MEDICAL_OBSERVATIONS_W,keys)

MEDICAL_OBSERVATIONS_W<-MEDICAL_OBSERVATIONS_W[mo_source_column=="COD_MORF_1" | mo_source_column=="COD_MORF_2" | mo_source_column=="COD_MORF_3" | mo_source_column=="COD_TOPOG",mo_source_column:=""]
MEDICAL_OBSERVATIONS_W<-MEDICAL_OBSERVATIONS_W[mo_source_column=="",mo_code:=mo_source_value]
MEDICAL_OBSERVATIONS_W<-MEDICAL_OBSERVATIONS_W[mo_source_column=="",`:=`(mo_record_vocabulary="SNOMED3",mo_source_value="")]

MEDICAL_OBSERVATIONS_AP<-MEDICAL_OBSERVATIONS_W[,`:=`(mo_source_table="AP", mo_unit="",mo_meaning="pathology_report",mo_origin="AP")]
rm(MEDICAL_OBSERVATIONS_, MEDICAL_OBSERVATIONS_W)

setnames(MEDICAL_OBSERVATIONS_AP, "IDUNI", "person_id")
setnames(MEDICAL_OBSERVATIONS_AP, "DAT_ACC", "mo_date")

MEDICAL_OBSERVATIONS_AP<-MEDICAL_OBSERVATIONS_AP[,`:=`(mo_date=as.Date(mo_date, "%d%b%Y"))] #10/09:added
MEDICAL_OBSERVATIONS_AP<-MEDICAL_OBSERVATIONS_AP[,`:=`(mo_date=format(mo_date, "%Y%m%d"))]

fwrite(MEDICAL_OBSERVATIONS_AP, paste0(diroutput,"/MEDICAL_OBSERVATIONS_AP.csv"), quote = "auto")

rm(AP)

# 13. Use VCN to populate VACCINES ----------------------------------------

## Specification table VACCINES - VCN: 
#The local tables feeding this CDM table are: VCN

VCN <- as.data.table(read_dta(paste0(dirinput,"/VCN.dta")))
setkeyv(VCN,"IDUNI")

VACCINES<-VCN[,`:=`(vx_type="",vx_text="", visit_occurrence_id="",origin_of_vx_record="VCN",meaning_of_vx_record="administration_of_vaccine_unspecified",vx_record_date=DATA_SOMMINISTRAZIONE, vx_admin_date=DATA_SOMMINISTRAZIONE)]

setnames(VACCINES, "IDUNI", "person_id")
#setnames(VACCINES, "DATA_SOMMINISTRAZIONE", "vx_record_date")	
#setnames(VACCINES, "DATA_INIZIO", "vx_record_date")
#setnames(VACCINES, "DATA_SOMMINISTRAZIONE", "vx_admin_date")
setnames(VACCINES, "COD_ATC5", "vx_atc")
#setnames(VACCINES, "COD_ATC_VACCINO", "vx_atc")
#setnames(VACCINES, "COD_PREST_VACCINO", "product_code")
setnames(VACCINES, "COD_PREST_VACCINO", "medicinal_product_id")
setnames(VACCINES, "DOSE_SOMMINISTRATA", "vx_dose")	
setnames(VACCINES, "TITOLARE_AIC", "vx_manufacturer")	
setnames(VACCINES, "NUM_LOTTO", "vx_lot_num")

VACCINES<-VACCINES[,.(person_id,vx_record_date,vx_admin_date,vx_atc,vx_type,vx_text,medicinal_product_id, origin_of_vx_record,meaning_of_vx_record,vx_dose,vx_manufacturer,vx_lot_num,visit_occurrence_id)] #product_code

VACCINES<-VACCINES[,`:=`(vx_record_date=format(vx_record_date, "%Y%m%d"), vx_admin_date=format(vx_admin_date, "%Y%m%d"))]
VACCINES<-VACCINES[,`:=`(medicinal_product_id=as.character(medicinal_product_id))]

fwrite(VACCINES, paste0(diroutput,"/VACCINES.csv"), quote = "auto")

rm(VCN,VACCINES)

# ** 14.	Use COD_FARMACI_SPF to populate PRODUCTS ----------------------------

## Specification table PRODUCTS -COD_FARMACI_SPF: 
#The origin tables feeding this target CDM table are: COD_FARMACI_SPF

COD_FARMACI_SPF <- as.data.table(read_dta(paste0(dirinput,"/COD_FARMACI_SPF.dta")))

#setkeyv(COD_FARMACI_SPF,"IDUNI")

PRODUCTS<-COD_FARMACI_SPF
PRODUCTS<-PRODUCTS[,`:=`( drug_form="",ingredient1_ATCcode="",ingredient2_ATCcode="",ingredient3_ATCcode="",amount_ingredient1="",amount_ingredient2="",amount_ingredient3="",amount_ingredient1_unit="", amount_ingredient2_unit="",amount_ingredient3_unit="")] #box_size="",box_size_unit="",

setnames(PRODUCTS,"COD_PRESTAZIONE","product_code")
setnames(PRODUCTS,"DESCRIZIONE","full_product_name")
setnames(PRODUCTS,"UNITA_POSOLOGIA","box_size")
setnames(PRODUCTS,"UM","box_size_unit")
setnames(PRODUCTS,"VDS","route_of_administration")
setnames(PRODUCTS,"COD_ATC5","product_ATCcode")
setnames(PRODUCTS,"TITOLARE_AIC","product_manufacturer")

# drop missing ATC!!!
PRODUCTS<-PRODUCTS[product_ATCcode!="",]

PRODUCTS<-PRODUCTS[,.(product_code,full_product_name,box_size,box_size_unit,drug_form,route_of_administration,product_ATCcode,ingredient1_ATCcode,ingredient2_ATCcode,ingredient3_ATCcode,amount_ingredient1, amount_ingredient2, amount_ingredient3, amount_ingredient1_unit,amount_ingredient2_unit,amount_ingredient3_unit,product_manufacturer)] 

fwrite(PRODUCTS, paste0(diroutput,"/PRODUCTS.csv"), quote = "auto")

rm(COD_FARMACI_SPF)

# 15.	Use CAP to populate PERSON_RELATIONSHIP -----------------------------

## Specification table PERSON_RELATIONSHIPS - CAP2: 
#The origin tables feeding this target CDM table are: CAP2

  #- CAP2:For each record of each child, perform record linkage with CAP1 and fill the column of this table as follows

for (source in cap){
  pippo <- as.data.table(read_dta(paste0(dirinput,source,".dta")))
  assign(source,pippo)
  setkeyv(pippo,"IDUNI")
}
rm(pippo)

PERSON_RELATIONSHIPS_<-merge(CAP2[,.(IDUNI,ID_CAP1_ARSNEW)], CAP1[,.(IDUNI,ID_CAP1_ARSNEW)], by="ID_CAP1_ARSNEW")

PERSON_RELATIONSHIPS_<-PERSON_RELATIONSHIPS_[,`:=`(origin_of_relationship="birth_registry",
  meaning_of_relationship="gestational_mother",method_of_linkage="deterministic")]

setnames(PERSON_RELATIONSHIPS_, "IDUNI.x","person_id")
setnames(PERSON_RELATIONSHIPS_, "IDUNI.y","related_id")

PERSON_RELATIONSHIPS<-PERSON_RELATIONSHIPS_[,.(person_id,related_id,origin_of_relationship,meaning_of_relationship,method_of_linkage)]

fwrite(PERSON_RELATIONSHIPS, paste0(diroutput,"/PERSON_RELATIONSHIPS.csv"), quote = "auto")

rm(PERSON_RELATIONSHIPS_, CAP2, CAP1)

# 16.	Use ARS_ANAG_MED_RES_storico to populate PERSONS and OBSERVATIONS_PERIODS --------

ANAFULL <- as.data.table(read_dta(paste0(dirinput,"/ANAFULL.dta")))
setkeyv(ANAFULL,"IDUNI")

## Specification table PERSONS: 
# The local tables feeding this CDM table are: ARS_ANAG_MED_RES_storico 
  #- ARS_ANAG_MED_RES_storico: Select all the distinct values of IDUNI having at least one record with COD_REGIONE=’090’; for each of them select the values as follows

PERSON_<-ANAFULL[,.(IDUNI, DATA_NASCITA, DATA_MORTE_MARSI,SESSO)]
PERSON<-unique(PERSON_)

PERSON<-PERSON[,DATA_NASCITA:=as.Date(DATA_NASCITA)]
PERSON<-PERSON[,DATA_MORTE_MARSI:=lubridate::ymd(DATA_MORTE_MARSI)]

PERSON<-PERSON[,`:=`(race="",country_of_birth="", quality="reliable")]
PERSON<-PERSON[,`:=`(day_of_birth=as.character(day(DATA_NASCITA)),
  month_of_birth=as.character(month(DATA_NASCITA)), year_of_birth=as.character(year(DATA_NASCITA)),
  day_of_death=as.character(day(DATA_MORTE_MARSI)), month_of_death=as.character(month(DATA_MORTE_MARSI)), year_of_death=as.character(year(DATA_MORTE_MARSI)))]
rm(PERSON_)
##21/12: change in modality of sex_at_instance_creation
PERSON<-PERSON[,SESSO:=as.character(SESSO)]
PERSON<-PERSON[SESSO=="1",SESSO:="M"][SESSO=="2",SESSO:="F"]

##22/04: added 0 if is not present, is mandatory to have2 digits
PERSON<-PERSON[nchar(day_of_birth)==1,day_of_birth:=paste0("0",day_of_birth)]
PERSON<-PERSON[nchar(month_of_birth)==1,month_of_birth:=paste0("0",month_of_birth)]
PERSON<-PERSON[nchar(year_of_birth)==1,year_of_birth:=paste0("0",year_of_birth)]
PERSON<-PERSON[nchar(month_of_death)==1,month_of_death:=paste0("0",month_of_death)]
PERSON<-PERSON[nchar(day_of_death)==1,day_of_death:=paste0("0",day_of_death)]
PERSON<-PERSON[nchar(year_of_birth)==1,year_of_birth:=paste0("0",year_of_birth)]

setnames(PERSON, "IDUNI","person_id")
#setnames(PERSON, "DATA_NASCITA","date_birth")
#setnames(PERSON, "DATA_MORTE_MARSI","date_death")
setnames(PERSON, "SESSO","sex_at_instance_creation")

# PERSONS<-PERSON[,`:=`(date_birth=format(date_birth, "%Y%m%d"),
#   date_death=format(date_death, "%Y%m%d"))]
PERSONS<-PERSON[,.(person_id,day_of_birth, month_of_birth,year_of_birth,day_of_death,month_of_death,year_of_death,sex_at_instance_creation,race, country_of_birth,quality)]

fwrite(PERSONS, paste0(diroutput,"/PERSONS.csv"), quote = "auto")
rm(PERSON)

## Specification table OBSERVATION_PERIODS: 
# The local tables feeding this CDM table are: ARS_ANAG_MED_RES_storico 

  #- ARS_ANAG_MED_RES_storico: For each record with COD_REGIONE=’090’, fill the column of this table as follows 

## added criteria of 60 days after date of birth as date of birth
OBSERVATION_PERIODS_<-ANAFULL[,.(IDUNI,INI_RECORD,FINE_RECORD, DATA_NASCITA)]

OBSERVATION_PERIODS_<-OBSERVATION_PERIODS_[,INI_RECORD:=as.Date(INI_RECORD)]
OBSERVATION_PERIODS_<-OBSERVATION_PERIODS_[,FINE_RECORD:=as.Date(FINE_RECORD)]
OBSERVATION_PERIODS_<-OBSERVATION_PERIODS_[,DATA_NASCITA:=as.Date(DATA_NASCITA)]

#OBSERVATION_PERIODS_[, diff:=INI_RECORD-DATA_NASCITA]
OBSERVATION_PERIODS_[DATA_NASCITA+60>=INI_RECORD & DATA_NASCITA<INI_RECORD,INI_RECORD:=DATA_NASCITA]


OBSERVATION_PERIODS_<-OBSERVATION_PERIODS_[,`:=`(op_origin="ARS_ANAG_MED_RES_storico",
  op_meaning="healthcare_office")]

setnames(OBSERVATION_PERIODS_, "IDUNI","person_id")
setnames(OBSERVATION_PERIODS_, "INI_RECORD","op_start_date")
setnames(OBSERVATION_PERIODS_, "FINE_RECORD","op_end_date")

OBSERVATION_PERIODS_<-OBSERVATION_PERIODS_[,`:=`(op_start_date= format(op_start_date, "%Y%m%d"), op_end_date= format(op_end_date, "%Y%m%d"))]
OBSERVATION_PERIODS<-OBSERVATION_PERIODS_[,.(person_id,op_start_date,op_end_date,op_meaning,op_origin)]
                                           
fwrite(OBSERVATION_PERIODS, paste0(diroutput,"/OBSERVATION_PERIODS.csv"), quote = "auto")

rm(ANAFULL, PERSONS, OBSERVATION_PERIODS_, OBSERVATION_PERIODS)


# # 17. Use COVID_DATASET to populate  SURVEY_ID, SURVEY_OBSERVATION -------------------------------------

## Specification table SURVEY_ID:
# For each record
#●	Create one record of SURVEY_ID for each row of COVIDDATASET, 
#●	Copy the values of COVIDDATASET into SURVEY_ID according to the following table for each maternal record

COVIDDATASET <- as.data.table(read_dta(paste0(dirinput,"COVIDDATASET.dta")))
setkeyv(COVIDDATASET,"IDUNI")

# select variables of interest
SURVEY_ID<-COVIDDATASET[,.(IDUNI, ID_CASO, DATA_PRELIEVO)]
# changed format in date
SURVEY_ID<-SURVEY_ID[,DATA_PRELIEVO:=as.Date(DATA_PRELIEVO)]
# add var according to CDM
SURVEY_ID<-SURVEY_ID[,survey_meaning:="covid_registry"]

# rename vars
setnames(SURVEY_ID, old="IDUNI", new = "person_id")
setnames(SURVEY_ID, old="DATA_PRELIEVO", new = "survey_date")
setnames(SURVEY_ID, old="ID_CASO", new = "survey_id")

SURVEY_ID<-SURVEY_ID[,`:=`(survey_date= format(survey_date, "%Y%m%d"))]

fwrite(SURVEY_ID, paste0(diroutput,"SURVEY_ID_COVIDDATASET.csv"), quote = "auto")



## Specification table SURVEY_OBSERVATION:

#For each record 
#●	Extract from SURVEY_ID (above) the corresponding value of survey_id
#●	Create a record of SURVEY_OBSERVATIONS for each non-empty cell in the list below; 
# a.	DATA_INIZIO_SINTOMI
# b.	DATA_PRELIEVO
# c.	DATA_DIAGNOSI
# d.	STATOCLINICO_PRELIEVO
# e.	STATOCLINICO_PIU_GRAVE
# f.	STATOCLINICO_PIU_RECENTE
# g.	DATA_STATOCLINICO_PIU_RECENTE
# h.	CASO_ATTIVO
# i.	RICOVERO
#for each of them, copy the content of the cell in the column obsevartion_source_value, and copy the other values as follows

# COVIDDATASET<-COVIDDATASET[,DATA_INIZIO_SINTOMI:=as.Date(DATA_INIZIO_SINTOMI)]
# COVIDDATASET<-COVIDDATASET[,DATA_DIAGNOSI:=as.Date(DATA_DIAGNOSI)]
# COVIDDATASET<-COVIDDATASET[,DATA_STATOCLINICO_PIU_RECENTE:=as.Date(DATA_STATOCLINICO_PIU_RECENTE)]

SURVEY_OBSERVATIONS<-COVIDDATASET[,.(IDUNI, ID_CASO, DATA_PRELIEVO)][,so_source_table:="COVIDDATASET"]

# change format of date in chr, to melt data
COVIDDATASET<-COVIDDATASET[,DATA_PRELIEVO:=as.character(DATA_PRELIEVO)]
COVIDDATASET<-COVIDDATASET[,DATA_INIZIO_SINTOMI:=as.character(DATA_INIZIO_SINTOMI)]
COVIDDATASET<-COVIDDATASET[,DATA_DIAGNOSI:=as.character(DATA_DIAGNOSI)]
COVIDDATASET<-COVIDDATASET[,DATA_STATOCLINICO_PIU_RECENTE:=as.character(DATA_STATOCLINICO_PIU_RECENTE)]

vars<- c("DATA_INIZIO_SINTOMI", "DATA_PRELIEVO", "DATA_DIAGNOSI",
         "STATOCLINICO_PRELIEVO", "STATOCLINICO_PIU_GRAVE", "STATOCLINICO_PIU_RECENTE",
         "DATA_STATOCLINICO_PIU_RECENTE", "CASO_ATTIVO", "RICOVERO")

COVIDDATASET_noempty<- COVIDDATASET[ !is.na(DATA_INIZIO_SINTOMI)| !is.na(DATA_PRELIEVO)| !is.na(DATA_DIAGNOSI)| STATOCLINICO_PRELIEVO!=""| STATOCLINICO_PIU_GRAVE!=""| STATOCLINICO_PIU_RECENTE!=""| !is.na(DATA_STATOCLINICO_PIU_RECENTE)| CASO_ATTIVO!=""| RICOVERO!=""| ID_CASO!="",]

COVIDDATASET_melt<-melt(COVIDDATASET_noempty, measure.vars = vars, variable.name="so_source_column", value.name = "so_source_value")

SURVEY_OBSERVATIONS_CD<-merge(COVIDDATASET_melt, SURVEY_OBSERVATIONS, by=c("IDUNI","ID_CASO"), all.x = T)

setnames(SURVEY_OBSERVATIONS_CD, old="IDUNI", new = "person_id")
setnames(SURVEY_OBSERVATIONS_CD, old="DATA_PRELIEVO", new = "so_date")
setnames(SURVEY_OBSERVATIONS_CD, old="ID_CASO", new = "survey_id")

SURVEY_OBSERVATIONS_CD<-SURVEY_OBSERVATIONS_CD[,so_meaning:="covid_registry"]
SURVEY_OBSERVATIONS_CD<-SURVEY_OBSERVATIONS_CD[,`:=`(so_date= format(so_date, "%Y%m%d"),so_unit="")]

SURVEY_OBSERVATIONS_CD<-SURVEY_OBSERVATIONS_CD[,.(person_id,so_date,so_source_table,so_source_column,so_source_value,survey_id)]

fwrite(SURVEY_OBSERVATIONS_CD, paste0(diroutput,"SURVEY_OBSERVATIONS_COVIDDATASET.csv"), quote = "auto")

rm(COVIDDATASET, COVIDDATASET_melt, COVIDDATASET_noempty, SURVEY_ID, SURVEY_OBSERVATIONS_CD, SURVEY_OBSERVATIONS)



