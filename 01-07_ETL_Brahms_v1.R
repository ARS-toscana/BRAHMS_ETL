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

# all files in dirinput
files<-sub('\\.csv$', '', list.files(dirinput))

# 1.	Use RICOVERI_OSPEDALIERI to populate CLINICAL_ITEMS, ENCOUNTERS, MISC_ITEMS --------

# keep only RICOVERI_OSPEDALIERI in dirinput
RICOVERI_OSPEDALIERI_table<- files[str_detect(files,"^RICOVERI_OSPEDALIERI")]

for (source in RICOVERI_OSPEDALIERI_table){
  print(source)
  
  pippo <- fread(paste0(dirinput,source,".csv"))
  setkeyv(pippo,"id")
  
  ## CLINICAL_ITEMS
  #Action:  For each record: create a record of CLINICAL_ITEMS;  copy the values of ESENZIONI into CLINICAL_ITEMS according to the following table
  #creating encounter_id before melt dataset
  pippo<-pippo[,encounter_id:=1:.N]
  
  CLINICAL_ITEMS_<-copy(pippo)[,.(id, data_a,codcmp,codcm1,codcm2,codcm3,codcm4,codcm5,intproc,intsec1,intsec2,intsec3,intsec4,intsec5,encounter_id)]

  # rename variables:
  setnames(CLINICAL_ITEMS_, old = "id", new = "person_id")
  setnames(CLINICAL_ITEMS_, old = "data_a", new = "clin_item_date")
  setnames(CLINICAL_ITEMS_, old = "codcmp", new = "codcm0")
  setnames(CLINICAL_ITEMS_, old = "intproc", new = "intsec0")
  
  # #creating encounter_id before melt dataset
  # CLINICAL_ITEMS_<-CLINICAL_ITEMS_[,encounter_id:=1:.N]
   
  CLINICAL_ITEMS__<-melt(CLINICAL_ITEMS_, measure= patterns("^codcm","^intsec"), variable.name = "ord", value.name = c("dia1","dia2"))[!(dia1=="" & dia2==""),] #dia1=diagnosis, dia2=procedure
  CLINICAL_ITEMS__<-CLINICAL_ITEMS__[,dia2:=as.character(dia2)]
  CLINICAL_ITEMS<-melt(CLINICAL_ITEMS__, measure= patterns("^dia"), variable.name = "clas", value.name = c("clin_item_code"))  
  CLINICAL_ITEMS<-CLINICAL_ITEMS[!is.na(clin_item_code) & clin_item_code!="",]
  
  # create others vars.
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,`:=`(clin_item_id= 1:.N,
                                       clin_item_src_id=paste0("RICOVERI_OSPEDALIERI_", 1:.N),
                                       clin_item_setting="1")]
                                       

  CLINICAL_ITEMS<-CLINICAL_ITEMS[clas=="dia2",clin_item_datatype:=4][clas=="dia1" & ord==1, clin_item_datatype:=1][clas=="dia1" & ord!=1, clin_item_datatype:=2]
  CLINICAL_ITEMS<-CLINICAL_ITEMS[clin_item_datatype==1 | clin_item_datatype==2,item_classification:='40'][clin_item_datatype==4, item_classification:='42']
  
  #make the table unique by person_id encounter_id clin_item_code clin_item_setting clin_item_datatype clin_item_date
  CLINICAL_ITEMS<-unique(CLINICAL_ITEMS, by=c("person_id","encounter_id","clin_item_code","clin_item_setting","clin_item_datatype","clin_item_date"))
  
  # transform right fotmat for person_id
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,person_id:=substr(person_id, 7,18)]
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,person_id:=as.numeric(person_id)]

  # keep only needed variables
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,.(clin_item_id,clin_item_src_id,person_id,encounter_id,clin_item_code,item_classification,clin_item_datatype,clin_item_setting,clin_item_date)]
  
  fwrite(CLINICAL_ITEMS, paste0(dirtemp,"/CLINICAL_ITEMS_",source,".csv"), quote = "auto")
  
  rm(CLINICAL_ITEMS,CLINICAL_ITEMS_,CLINICAL_ITEMS__)
  
  ##ENCOUNTERS: 
  #Action:  For each record: create a record of ENCOUNTERS and label the records with a unique numeric code stored in encounter id (primary key); copy the values of RICOVERI OSPEDALIERI into ENCOUNTERS according to the following table
  ENCOUNTERS<-copy(pippo)
  
  ENCOUNTERS<-ENCOUNTERS[,`:=`(encounter_src_id=paste0("RICOVERI_OSPEDALIERI_", 1:.N),
                               encounter_misc="99", 
                               encounter_setting=1,
                               encounter_start_date=data_a)] #encounter_id=1:.N,

  
  ENCOUNTERS<-ENCOUNTERS[,encounter_end_date:=min(data_d, end_study, na.rm=T),by="encounter_src_id"]
  ENCOUNTERS<-ENCOUNTERS[(tipdim==0 | tipdim==1),encounter_discharge_status:=1][tipdim==2 ,encounter_discharge_status:=2] [is.na(tipdim) |tipdim==99 | tipdim==3, encounter_discharge_status:=99]
  ENCOUNTERS<-ENCOUNTERS[str_detect(repartodim,paste0("^",derm_specility_sdo)) ,encounter_derm_specialty:=1][is.na(encounter_derm_specialty), encounter_derm_specialty:=0] # solo reparto dimissione
  ENCOUNTERS<-ENCOUNTERS[encounter_derm_specialty==1, encounter_specialty:= 1]
  ENCOUNTERS<-ENCOUNTERS[str_detect(repartodim,paste0("^",int_med_speciality_sdo)), encounter_specialty:= 2]
  ENCOUNTERS<-ENCOUNTERS[str_detect(repartodim,paste0("^",surg_speciality_sdo[1])) | str_detect(repartodim,paste0("^",surg_speciality_sdo[2])) | str_detect(repartodim,paste0("^",surg_speciality_sdo[3])) | str_detect(repartodim,paste0("^",surg_speciality_sdo[4])) | str_detect(repartodim,paste0("^",surg_speciality_sdo[5])) | str_detect(repartodim,paste0("^",surg_speciality_sdo[6])) | str_detect(repartodim,paste0("^",surg_speciality_sdo[7])) | str_detect(repartodim,paste0("^",surg_speciality_sdo[8])), encounter_specialty:= 3]
  ENCOUNTERS<-ENCOUNTERS[str_detect(repartodim,paste0("^",onco_speciality_sdo)), encounter_specialty:= 4]
  ENCOUNTERS<-ENCOUNTERS[str_detect(repartodim,paste0("^",psi_speciality_sdo)), encounter_specialty:= 5]
  ENCOUNTERS<-ENCOUNTERS[is.na(encounter_specialty), encounter_specialty:= 99]
  
  # rename variables:
  setnames(ENCOUNTERS, old = "id", new = "person_id")
  
  # transform right format for person_id
  ENCOUNTERS<-ENCOUNTERS[,person_id:=substr(person_id, 7,18)]
  ENCOUNTERS<-ENCOUNTERS[,person_id:=as.numeric(person_id)]

  ENCOUNTERS<-ENCOUNTERS[,.(encounter_id, encounter_src_id, person_id,encounter_setting, encounter_start_date, encounter_end_date, encounter_derm_specialty,encounter_specialty, encounter_discharge_status,encounter_misc)]
  
  fwrite(ENCOUNTERS, paste0(dirtemp,"/ENCOUNTERS_",source,".csv"), quote = "auto")
  
}

rm(pippo, ENCOUNTERS)  



# 2.  Use SPECIALISTICA	to populate ENCOUNTERS, CLINICAL_ITEMS --------------
SPECIALISTICA_table<- files[str_detect(files,"^SPECIALISTICA")]

for (source in SPECIALISTICA_table){
  print(source)
  
  pippo <- fread(paste0(dirinput,source,".csv"))
  setkeyv(pippo,"id")
 
  ## CLINICAL_ITEMS
  #For each record: create a record of CLINICAL_ITEMS;  copy the values of SPECIALISTICA into CLINICAL_ITEMS according to the following table
  
  pippo<-pippo[,encounter_id:=1:.N]
  CLINICAL_ITEMS<-copy(pippo)
  
  # rename variables:
  setnames(CLINICAL_ITEMS, old = "id", new = "person_id")
  setnames(CLINICAL_ITEMS, old = "codprest", new = "clin_item_code")
  setnames(CLINICAL_ITEMS, old = "dataprest", new = "clin_item_date")
  
  CLINICAL_ITEMS<-CLINICAL_ITEMS[!is.na(clin_item_code) & clin_item_code !="",]
  
  # create others vars.
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,`:=`(clin_item_id =1:.N,
                                       clin_item_src_id=paste0("SPECIALISTICA_", 1:.N),
                                       item_classification="41",
                                       clin_item_datatype="4",
                                       clin_item_setting="6")]
  
  #make the table unique by person_id encounter_id clin_item_code clin_item_setting clin_item_datatype clin_item_date
  CLINICAL_ITEMS<-unique(CLINICAL_ITEMS, by=c("person_id","encounter_id","clin_item_code","clin_item_setting","clin_item_datatype","clin_item_date"))
  
  # transform right fotmat for person_id
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,person_id:=substr(person_id, 7,18)]
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,person_id:=as.numeric(person_id)]

  # keep only needed variables
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,.(clin_item_id,clin_item_src_id,person_id,encounter_id,clin_item_code,item_classification,clin_item_datatype,clin_item_setting,clin_item_date)]
  
  fwrite(CLINICAL_ITEMS, paste0(dirtemp,"/CLINICAL_ITEMS_",source,".csv"), quote = "auto")
  
  ##ENCOUNTERS: 
  #For each record: create a record of ENCOUNTERS and label the records with a unique code stored in encounter id (primary key); copy the values of SPECIALISTICA into ENCOUNTERS according to the following table
  ENCOUNTERS<-copy(pippo)
  
  ENCOUNTERS<-ENCOUNTERS[,`:=`(encounter_src_id=paste0("SPECIALISTICA_", 1:.N),
                               encounter_start_date=dataprest,
                               encounter_setting=6,
                               encounter_misc="99",
                               encounter_discharge_status=99)] #encounter_id=1:.N, 
  
  ENCOUNTERS<-ENCOUNTERS[,encounter_end_date:=min(dataprest, end_study, na.rm=T),by="encounter_src_id"]
  ENCOUNTERS<-ENCOUNTERS[codbranca %in% derm_specility_spa ,encounter_derm_specialty:=1][is.na(encounter_derm_specialty), encounter_derm_specialty:=0] 
  ENCOUNTERS<-ENCOUNTERS[encounter_derm_specialty==1, encounter_specialty:= 1]
  ENCOUNTERS<-ENCOUNTERS[str_detect(codbranca,paste0("^",int_med_speciality_spa)), encounter_specialty:= 2]
  ENCOUNTERS<-ENCOUNTERS[str_detect(codbranca,paste0("^",surg_speciality_spa[1])) | str_detect(codbranca,paste0("^",surg_speciality_spa[2])) | str_detect(codbranca,paste0("^",surg_speciality_spa[3])) | str_detect(codbranca,paste0("^",surg_speciality_spa[4])) | str_detect(codbranca,paste0("^",surg_speciality_spa[5])) | str_detect(codbranca,paste0("^",surg_speciality_spa[6])) | str_detect(codbranca,paste0("^",surg_speciality_spa[7])) | str_detect(codbranca,paste0("^",surg_speciality_spa[8])), encounter_specialty:= 3]
  ENCOUNTERS<-ENCOUNTERS[str_detect(codbranca,paste0("^",onco_speciality_spa)), encounter_specialty:= 4]
  ENCOUNTERS<-ENCOUNTERS[str_detect(codbranca,paste0("^",psi_speciality_spa)), encounter_specialty:= 5]
  ENCOUNTERS<-ENCOUNTERS[is.na(encounter_specialty), encounter_specialty:= 99]
  
  
  setnames(ENCOUNTERS, old = "id", new = "person_id")
  # transform right format for person_id
  ENCOUNTERS<-ENCOUNTERS[,person_id:=substr(person_id, 7,18)]
  ENCOUNTERS<-ENCOUNTERS[,person_id:=as.numeric(person_id)]

  ENCOUNTERS<-ENCOUNTERS[,.(encounter_id, encounter_src_id, person_id, encounter_setting,encounter_start_date, encounter_end_date, encounter_derm_specialty,encounter_specialty, encounter_discharge_status, encounter_misc)] 
  
  fwrite(ENCOUNTERS, paste0(dirtemp,"/ENCOUNTERS_",source,".csv"), quote = "auto")
  
  rm(pippo)
  
  
}

rm(CLINICAL_ITEMS, ENCOUNTERS)


# 3.  Use PRONTO_SOCCORSO to populate CLINICAL_ITEMS, ENCOUNTERS ------------------------------------------

PRONTO_SOCCORSO_table<- files[str_detect(files,"^PRONTO_SOCCORSO")]

for (source in PRONTO_SOCCORSO_table){
  print(source)
  
  pippo <- fread(paste0(dirinput,source,".csv"))
  setkeyv(pippo,"id")
  

  ## CLINICAL_ITEMS
  #Action:  For each record: create a record of CLINICAL_ITEMS;  copy the values of ESENZIONI into CLINICAL_ITEMS according to the following table
  pippo<-pippo[,encounter_id:=1:.N]
  CLINICAL_ITEMS<-copy(pippo)
  
  # rename variables:
  setnames(CLINICAL_ITEMS, old = "id", new = "person_id")
  setnames(CLINICAL_ITEMS, old = "codcmp", new = "clin_item_code")
  setnames(CLINICAL_ITEMS, old = "data_a", new = "clin_item_date")
  
  CLINICAL_ITEMS<-CLINICAL_ITEMS[!is.na(clin_item_code) & clin_item_code !="",]
  # create others vars.
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,`:=`(clin_item_id =1:.N,
                                       clin_item_src_id=paste0("PRONTO_SOCCORSO_", 1:.N),
                                       item_classification="40",
                                       clin_item_datatype="1",
                                       clin_item_setting="3"  )]
  
  #make the table unique by person_id encounter_id clin_item_code clin_item_setting clin_item_datatype clin_item_date
  CLINICAL_ITEMS<-unique(CLINICAL_ITEMS, by=c("person_id","encounter_id","clin_item_code","clin_item_setting","clin_item_datatype","clin_item_date"))
  
  # transform right fotmat for person_id
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,person_id:=substr(person_id, 7,18)]
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,person_id:=as.numeric(person_id)]

  # keep only needed variables
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,.(clin_item_id,clin_item_src_id,person_id,encounter_id,clin_item_code,item_classification,clin_item_datatype,clin_item_setting,clin_item_date)]
  
  fwrite(CLINICAL_ITEMS, paste0(dirtemp,"/CLINICAL_ITEMS_",source,".csv"), quote = "auto")
  
  ##ENCOUNTERS: 
  #Action:  For each record: create a record of ENCOUNTERS and label the records with a unique numeric code stored in encounter id (primary key); copy the values of RICOVERI OSPEDALIERI into ENCOUNTERS according to the following table
  ENCOUNTERS<-copy(pippo)
  
  ENCOUNTERS<-ENCOUNTERS[,`:=`(encounter_src_id=paste0("PRONTO_SOCCORSO_",1:.N),
                               encounter_start_date=data_a,
                               encounter_setting=3,
                               encounter_derm_specialty=99,
                               encounter_specialty=88,
                               encounter_misc="99" )]
  
  ENCOUNTERS<-ENCOUNTERS[,encounter_end_date:=min(data_d, end_study, na.rm=T),by="encounter_src_id"]
  ENCOUNTERS<-ENCOUNTERS[(tipdim=="0" | tipdim=="1"),encounter_discharge_status:="1"][tipdim=="2" ,encounter_discharge_status:="2"] [tipdim=="" |tipdim=="99" | tipdim=="3", encounter_discharge_status:="99"]
  ENCOUNTERS<-ENCOUNTERS[,encounter_discharge_status:=as.numeric(encounter_discharge_status)]
  
  setnames(ENCOUNTERS, old = "id", new = "person_id")
    # transform right format for person_id
  ENCOUNTERS<-ENCOUNTERS[,person_id:=substr(person_id, 7,18)]
  ENCOUNTERS<-ENCOUNTERS[,person_id:=as.numeric(person_id)]

  ENCOUNTERS<-ENCOUNTERS[,.(encounter_id, encounter_src_id, person_id,encounter_setting,encounter_start_date, encounter_end_date, encounter_derm_specialty,encounter_specialty, encounter_discharge_status,encounter_misc)] 
  
  fwrite(ENCOUNTERS, paste0(dirtemp,"/ENCOUNTERS_",source,".csv"), quote = "auto")
  
  
  rm(pippo)
}

rm(CLINICAL_ITEMS)

# 4.  Use ESENZIONI to populate CLINICAL_ITEMS, ENCOUNTERS --OK! ------------------------------------------

ESENZIONI_table<- files[str_detect(files,"^ESENZIONI")]


for (source in ESENZIONI_table){
  pippo <- fread(paste0(dirinput,source,".csv"))
  setkeyv(pippo,"id")
  
  ## CLINICAL_ITEMS
  #Action:  For each record: create a record of CLINICAL_ITEMS;  copy the values of ESENZIONI into CLINICAL_ITEMS according to the following table
  pippo<-pippo[,encounter_id:=1:.N]
  CLINICAL_ITEMS<-copy(pippo)
  
  # rename variables:
  setnames(CLINICAL_ITEMS, old = "id", new = "person_id")
  setnames(CLINICAL_ITEMS, old = "ese_cod", new = "clin_item_code")
  setnames(CLINICAL_ITEMS, old = "datai", new = "clin_item_date")
  
  CLINICAL_ITEMS<-CLINICAL_ITEMS[!is.na(clin_item_code) & clin_item_code !="",]
  
  # create others vars.
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,`:=`(clin_item_id =1:.N,
                                       clin_item_src_id=paste0("ESENZIONI_",1:.N),
                                       item_classification="40", 
                                       clin_item_datatype="3",
                                       clin_item_setting="10"  )]
  
  #make the table unique by person_id encounter_id clin_item_code clin_item_setting clin_item_datatype clin_item_date
  CLINICAL_ITEMS<-unique(CLINICAL_ITEMS, by=c("person_id","encounter_id","clin_item_code","clin_item_setting","clin_item_datatype","clin_item_date"))
  
  # transform right fotmat for person_id
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,person_id:=substr(person_id, 7,18)]
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,person_id:=as.numeric(person_id)]

  # keep only needed variables
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,.(clin_item_id,clin_item_src_id,person_id,encounter_id,clin_item_code,item_classification,clin_item_datatype,clin_item_setting,clin_item_date)]
  
  fwrite(CLINICAL_ITEMS, paste0(dirtemp,"/CLINICAL_ITEMS_",source,".csv"), quote = "auto")
  
  
  ##ENCOUNTERS: 
  #Action:  For each record: create a record of ENCOUNTERS and label the records with a unique numeric code stored in encounter id (primary key); copy the values of RICOVERI OSPEDALIERI into ENCOUNTERS according to the following table
  ENCOUNTERS<-copy(pippo)
  
  ENCOUNTERS<-ENCOUNTERS[,`:=`(encounter_src_id=paste0("ESENZIONI_", 1:.N),
                               encounter_start_date=datai,
                               encounter_setting=88,
                               encounter_discharge_status=99,
                               encounter_derm_specialty=99,
                               encounter_specialty=99,
                               encounter_misc="99" )]
  
  ENCOUNTERS<-ENCOUNTERS[,encounter_end_date:=min(dataf, end_study, na.rm=T),by="encounter_src_id"]
  
  setnames(ENCOUNTERS, old = "id", new = "person_id")
  # transform right format for person_id
  ENCOUNTERS<-ENCOUNTERS[,person_id:=substr(person_id, 7,18)]
  ENCOUNTERS<-ENCOUNTERS[,person_id:=as.numeric(person_id)]

  ENCOUNTERS<-ENCOUNTERS[,.(encounter_id, encounter_src_id, person_id,encounter_setting, encounter_start_date, encounter_end_date, encounter_derm_specialty,encounter_specialty, encounter_discharge_status,encounter_misc)] 
  
  fwrite(ENCOUNTERS, paste0(dirtemp,"/ENCOUNTERS_",source,".csv"), quote = "auto")
  
  
  rm(pippo)
  
}

rm(CLINICAL_ITEMS)

# 5.  Use PRESCRIZIONI_FARMACI to populate DRUG_ITEMS --OK!  --------------------------------------------

PRESCRIZIONI_FARMACI <- data.table()
for (i in 1:length(files)) {
  if (str_detect(files[i],"^PRESCRIZIONI_FARMACI")) {  
    temp <- fread(paste0(dirinput,files[i],".csv"), colClasses = list( character="id"))
    PRESCRIZIONI_FARMACI <- rbind(PRESCRIZIONI_FARMACI, temp,fill=T)
    rm(temp)
  }
}

setkeyv(PRESCRIZIONI_FARMACI,"id")

## DRUG_ITEMS
  #Action: For each record: create a record of DRUG_ITEMS; copy the values of PRESCRIZIONI_FARMACI into DRUG_ITEMS according to the following table
DRUG_ITEMS<-copy(PRESCRIZIONI_FARMACI)[,drug_item_id:=1:.N]

# rename variables:
setnames(DRUG_ITEMS, old = "id", new = "person_id")
setnames(DRUG_ITEMS, old = "datasped", new = "drug_item_date")
setnames(DRUG_ITEMS, old = "atc", new = "drug_item_code")
setnames(DRUG_ITEMS, old = "aic", new = "drug_item_npc")
setnames(DRUG_ITEMS, old = "pezzi", new = "drug_item_quantity")
setnames(DRUG_ITEMS, old = "ddd", new = "drug_item_total_ddd")

DRUG_ITEMS<-DRUG_ITEMS[!is.na(drug_item_date),]
DRUG_ITEMS<-DRUG_ITEMS[,person_id:=substr(person_id, 7,18)]
DRUG_ITEMS<-DRUG_ITEMS[,person_id:=as.numeric(person_id)]
                         
DRUG_ITEMS<-DRUG_ITEMS[,`:=`(drug_item_src_id="",
                             encounter_id="",
                             item_classification=1,
                             drug_item_datatype=1,
                             drug_item_dosage_available=0,
                             drug_item_presc_specialty=99,
                             drug_item_quantity_unit="",
                             drug_item_presc_daily_dose="",
                             drug_item_days_supply="",
                             drug_item_strength="",
                             drug_item_strength_unit="",
                             drug_item_adm_route="",
                             drug_item_dose_form="")] 
#drug_item_setting=‘4’ if from community pharmacy (irrespective of which organization purchased the drug) 
                  #‘5’ if from hospital pharmacy
DRUG_ITEMS<-DRUG_ITEMS[origine=="FED",drug_item_setting:=5]
DRUG_ITEMS<-DRUG_ITEMS[origine=="SPF",drug_item_setting:=4]

# keep only needed variables
DRUG_ITEMS<-DRUG_ITEMS[,.(drug_item_id,drug_item_src_id,person_id,encounter_id,drug_item_code,item_classification,drug_item_datatype,drug_item_setting,drug_item_npc,drug_item_presc_specialty,drug_item_date,drug_item_dosage_available,drug_item_quantity,drug_item_quantity_unit,drug_item_total_ddd,drug_item_presc_daily_dose,drug_item_days_supply,drug_item_strength,drug_item_strength_unit,drug_item_adm_route,drug_item_dose_form)] 


fwrite(DRUG_ITEMS, paste0(dirtemp,"/DRUG_ITEMS.csv"), quote = "auto")

rm(DRUG_ITEMS, PRESCRIZIONI_FARMACI)

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
PERSONS<-copy(ANAGRAFE_ASSISTITI)
#PERSONS<-PERSONS[,person_id:=1:.N]
# transform right fotmat for person_id
PERSONS<-PERSONS[,person_id:=substr(id, 7,18)]
PERSONS<-PERSONS[,person_id:=as.numeric(person_id)]


#renamed vars
setnames(PERSONS,"id","person_id_src")
setnames(PERSONS,"datanas","birth_date")
setnames(PERSONS,"datadec","death_date")
setnames(PERSONS,"sesso","sex")


# keep only needed vars.
PERSONS<-PERSONS[,.(person_id,person_id_src,birth_date,sex,death_date)]

fwrite(PERSONS, paste0(dirtemp,"/PERSONS.csv"), quote = "auto")



## OBSERVATION_PERIODS: 
  # Action: one row of ANAGRAFE_ASSISTITI generates one row of OBSERVATION_PERIODS (multiple observations per person are possible)

OBSERVATION_PERIODS<-copy(ANAGRAFE_ASSISTITI)[,source:="ALL"]


#renamed vars
setnames(OBSERVATION_PERIODS,"id","person_id")
setnames(OBSERVATION_PERIODS,"data_inizioass","obs_period_start_date")
setnames(OBSERVATION_PERIODS,"data_fineass","obs_period_end_date")
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[,obs_period_end_date:=min(obs_period_end_date, end_study, na.rm = T),by="person_id"]
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[,obs_period_id:=1:.N]

# create obs_period_end_reason
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[obs_period_end_date==date_end, obs_period_end_reason:=1]
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[obs_period_end_date==datadec, obs_period_end_reason:=2]
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[(obs_period_end_date!=datadec | obs_period_end_date!=date_end ), obs_period_end_reason:=3] #| is.na(datadec)

# transform right fotmat for person_id
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[,person_id:=substr(person_id, 7,18)]
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[,person_id:=as.numeric(person_id)]

# keep only needed vars.
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[,.(obs_period_id,person_id,source,obs_period_start_date,obs_period_end_date,obs_period_end_reason)]

fwrite(OBSERVATION_PERIODS, paste0(dirtemp,"/OBSERVATION_PERIODS.csv"), quote = "auto")

rm(ANAGRAFE_ASSISTITI, PERSONS, OBSERVATION_PERIODS)



# 7.  Keep all together CDM tables -----------------------------------------

# all files in dirinput
files_temp<-sub('\\.csv$', '', list.files(dirtemp))

#PERSONS
PERSONS_table<- files_temp[str_detect(files_temp,"^PERSONS")]

PERSONS_all<-c()
for (source in PERSONS_table){
  
  print(source)
  pippo <- fread(paste0(dirtemp,source,".csv"))
  
  PERSONS_all<-rbind(PERSONS_all, pippo)
  rm(pippo)
}  

fwrite(PERSONS_all, paste0(diroutput,"/PERSONS.csv"), quote = "auto")


#OBSERVATION_PERIODS
OBSERVATION_PERIODS_table<- files_temp[str_detect(files_temp,"^OBSERVATION_PERIODS")]

OBSERVATION_PERIODS_all<-c()
for (source in OBSERVATION_PERIODS_table){
  
  print(source)
  pippo <- fread(paste0(dirtemp,source,".csv"))
  
  OBSERVATION_PERIODS_all<-rbind(OBSERVATION_PERIODS_all, pippo)
  rm(pippo)
}  

fwrite(OBSERVATION_PERIODS_all, paste0(diroutput,"/OBSERVATION_PERIODS.csv"), quote = "auto")

#ENCOUNTERS
ENCOUNTERS_table<- files_temp[str_detect(files_temp,"^ENCOUNTERS")]

ENCOUNTERS_all<-c()
for (source in ENCOUNTERS_table){
  
  print(source)
  pippo <- fread(paste0(dirtemp,source,".csv"))
  
  ENCOUNTERS_all<-rbind(ENCOUNTERS_all, pippo)
  rm(pippo)
}  

ENCOUNTERS_all[,encounter_id:=1:.N]
# check for date 
ENCOUNTERS_all<-ENCOUNTERS_all[!(encounter_start_date>encounter_end_date),]
#check whether person_id are all in PERSONS
ENCOUNTERS_all<-ENCOUNTERS_all[person_id%in%unique(PERSONS_all$person_id)]

fwrite(ENCOUNTERS_all, paste0(diroutput,"/ENCOUNTERS.csv"), quote = "auto")


# CLINICAL_ITEMS, 
CLINICAL_ITEMS_table<- files_temp[str_detect(files_temp,"^CLINICAL_ITEMS")]

CLINICAL_ITEMS_all<-c()
for (source in CLINICAL_ITEMS_table){
  
  print(source)
  pippo <- fread(paste0(dirtemp,source,".csv"))
  
  CLINICAL_ITEMS_all<-rbind(CLINICAL_ITEMS_all, pippo)
  rm(pippo)
}  


CLINICAL_ITEMS_all<-CLINICAL_ITEMS_all[,clin_item_id:= 1:.N]
#check whether person_id are all in PERSONS
CLINICAL_ITEMS_all<-CLINICAL_ITEMS_all[person_id%in%unique(PERSONS_all$person_id)]
#check whether encounter_id are all in ENCOUNTERS
CLINICAL_ITEMS_all<-CLINICAL_ITEMS_all[encounter_id%in%unique(ENCOUNTERS_all$encounter_id)]

fwrite(CLINICAL_ITEMS_all, paste0(diroutput,"/CLINICAL_ITEMS.csv"), quote = "auto")


#MISC_ITEMS
MISC_ITEMS_table<- files_temp[str_detect(files_temp,"^MISC_ITEMS")]

MISC_ITEMS_all<-c()
for (source in MISC_ITEMS_table){
  
  print(source)
  pippo <- fread(paste0(dirtemp,source,".csv"))
  
  MISC_ITEMS_all<-rbind(MISC_ITEMS_all, pippo)
  rm(pippo)
}  

#check whether person_id are all in PERSONS
MISC_ITEMS_all<-MISC_ITEMS_all[person_id%in%unique(PERSONS_all$person_id)]

fwrite(MISC_ITEMS_all, paste0(diroutput,"/MISC_ITEMS.csv"), quote = "auto")
rm(MISC_ITEMS_all)


#DRUG_ITEMS
DRUG_ITEMS_table<- files_temp[str_detect(files_temp,"^DRUG_ITEMS")]

DRUG_ITEMS_all<-c()
for (source in DRUG_ITEMS_table){
  
  print(source)
  pippo <- fread(paste0(dirtemp,source,".csv"))
  
  DRUG_ITEMS_all<-rbind(DRUG_ITEMS_all, pippo)
  rm(pippo)
}  

#check whether person_id are all in PERSONS
DRUG_ITEMS_all<-DRUG_ITEMS_all[person_id%in%unique(PERSONS_all$person_id)]

fwrite(DRUG_ITEMS_all, paste0(diroutput,"/DRUG_ITEMS.csv"), quote = "auto")
rm(DRUG_ITEMS_all)

rm(PERSONS_all, OBSERVATION_PERIODS_all, ENCOUNTERS_all, CLINICAL_ITEMS_all, ENCOUNTERS, PERSONS)


## META* tables

#META_VERSIONING
META_VERSIONING <- fread(paste0(dirtemp,"META_VERSIONING.csv"))
fwrite(META_VERSIONING, paste0(diroutput,"/META_VERSIONING.csv"), quote = "auto")
rm(META_VERSIONING)

#META_DATE_PRECISION
META_DATE_PRECISION <- fread(paste0(dirtemp,"META_DATE_PRECISION.csv"))
fwrite(META_DATE_PRECISION, paste0(diroutput,"/META_DATE_PRECISION.csv"), quote = "auto")
rm(META_DATE_PRECISION)

#META_DATA_SOURCES
META_DATA_SOURCES <- fread(paste0(dirtemp,"META_DATA_SOURCES.csv"))
fwrite(META_DATA_SOURCES, paste0(diroutput,"/META_DATA_SOURCES.csv"), quote = "auto")
rm(META_DATA_SOURCES)
