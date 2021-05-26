#################################################################################################
## ETL TheShinISS -> BRAHMS ####

# Version 1.0
# 26-05-2021
# Author: CB 

#################################################################################################

rm(list=ls(all.names=TRUE))

# set the directory where the file is saved as the working directory
thisdir<-dirname(rstudioapi::getSourceEditorContext()$path)
setwd(thisdir)

#load parameters
source(paste0(thisdir,"/parameters_toBrahms.R"))
setwd(thisdir)


files<-sub('\\.csv$', '', list.files(dirinput))

# Read and check data -----------------------------------------------------

# for (source in alldatasets){
#   pippo <- as.data.table(read_dta(paste0(thisdir,"/input_corrected/",source,".dta")))
#   assign(source,pippo)
#   setkeyv(pippo,"IDUNI")
# }
# rm(pippo)

# For each row of table 3.1 implement the corrisponding specification table



# 1.	Use RICOVERI_OSPEDALIERI to populate CLINICAL_ITEMS, ENCOUNTERS,MISC_ITEMS --------

files<-sub('\\.RData$', '', list.files(dirinput))

for (i in 1:length(files)) {
  if (str_detect(files[i],"^RICOVERI_OSPEDALIERI")) { 
    load(paste0(dirtemp,files[i],".RData")) 
  }
}




fwrite(MEDICAL_OBSERVATIONS, paste0(diroutput,"/MEDICAL_OBSERVATIONS_SDO.csv"), quote = "auto")

rm(list=ls(pattern="^RICOVERI"))



# 2.  Use SPECIALISTICA	to populate ENCOUNTERS, CLINICAL_ITEMS --------------

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

# 3.  Use PRONTO_SOCCORSO to populate CLINICAL_ITEMS, ENCOUNTERS ------------------------------------------

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

# 4.  Use ESENZIONI to populate CLINICAL_ITEMS ------------------------------------------

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

# 5.  Use PRESCRIZIONI_FARMACI to populate DRUG_ITEMS  --------------------------------------------

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

# 6.	Use ANAGRAFE_ASSISTITI to populate PERSONS, OBSERVATION_PERIODS --------

ANAGRAFE_ASSISTITI <- data.table()
for (i in 1:length(files)) {
  if (str_detect(files[i],"^ANAGRAFE_ASSISTITI")) {  
    temp <- fread(paste0(dirinput,files[i],".csv"), colClasses = list( character="id"))
    ANAGRAFE_ASSISTITI <- rbind(ANAGRAFE_ASSISTITI, temp,fill=T)
    rm(temp)
  }
}

setkeyv(ANAGRAFE_ASSISTITI,"id")

## PERSONS:
  # Action: all rows of ANAGRAFE_ASSISTITI for the same person_id have the same variables below, so one single row of PERSONS is generated
PERSONS<-ANAGRAFE_ASSISTITI

#create person_id as number that identifier uniquly person, used to link across tables. (Primary key)
PERSONS<-PERSONS[,person_id:=1:.N]
PERSONS<-copy(ANAGRAFE_ASSISTITI)

#renamed vars
setnames(PERSONS,"id","person_id_src")
setnames(PERSONS,"datanas","birth_date")
setnames(PERSONS,"datadec","death_date")
setnames(PERSONS,"sesso","sex")

# keep only needed vars.
PERSONS<-PERSONS[,.(person_id,person_id_src,birth_date,sex,death_date)]

fwrite(PERSONS, paste0(diroutput,"/PERSONS.csv"), quote = "auto")



## OBSERVATION_PERIODS: 
  # Action: one row of ANAGRAFE_ASSISTITI generates one row of OBSERVATION_PERIODS (multiple observations per person are possible)

OBSERVATION_PERIODS<-copy(ANAGRAFE_ASSISTITI)[,source:="OBSERVATION_PERIODS"]


#renamed vars
setnames(OBSERVATION_PERIODS,"id","obs_period_id")
setnames(OBSERVATION_PERIODS,"data_inizioass","obs_period_start_date")
setnames(OBSERVATION_PERIODS,"data_fineass","obs_period_end_date")

# obs_period_end_reason		1 = if obs_period_end_date is the date when the data has ended
# 2 = if obs_period_end_date is = to datadec in ANANAGRAFE ASSISTITI
# 3 = if obs_period_end_date is not the date when the data has ended, and is not = to datadec



# keep only needed vars.
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[,.(obs_period_id,person_id,source,obs_period_start_date,obs_period_end_date,obs_period_end_reason)]

fwrite(OBSERVATION_PERIODS, paste0(diroutput,"/OBSERVATION_PERIODS.csv"), quote = "auto")

rm(ANAGRAFE_ASSISTITI, PERSONS, OBSERVATION_PERIODS)


