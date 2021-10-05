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


# 1.	Use RICOVERI_OSPEDALIERI to populate CLINICAL_ITEMS, ENCOUNTERS --OK! (NO:MISC_ITEMS) --------

# keep only RICOVERI_OSPEDALIERI in dirinput
RICOVERI_OSPEDALIERI_table<- files[str_detect(files,"^RICOVERI_OSPEDALIERI")]

for (source in RICOVERI_OSPEDALIERI_table){
  pippo <- fread(paste0(dirinput,source,".csv"))
  setkeyv(pippo,"id")
  
  
  ##ENCOUNTERS: 
  #Action:  For each record: create a record of ENCOUNTERS and label the records with a unique numeric code stored in encounter id (primary key); copy the values of RICOVERI OSPEDALIERI into ENCOUNTERS according to the following table
  ENCOUNTERS_all<-copy(pippo)
  
  ENCOUNTERS_all<-ENCOUNTERS_all[,`:=`(encounter_id=1:.N, 
                               encounter_src_id=paste0("RICOVERI_OSPEDALIERI_", 1:.N),
                               encounter_misc="99",
                               encounter_start_date=data_a)]
  
  ENCOUNTERS_all<-ENCOUNTERS_all[,encounter_end_date:=min(data_d, end_study), by=c("encounter_id")]

  ENCOUNTERS_all<-ENCOUNTERS_all[(tipdim==0 | tipdim==1),
                         encounter_discharge_status:=1][tipdim==2 ,
                                                        encounter_discharge_status:=2] [is.na(tipdim) |tipdim==99, 
                                                                                        encounter_discharge_status:=99]
  ENCOUNTERS_all<-ENCOUNTERS_all[repartoam %chin% derm_specility_sdo | repartodim %chin% derm_specility_sdo, encounter_derm_specialty:=1][is.na(encounter_derm_specialty), 
                                                      encounter_derm_specialty:=0] # repartotras1, repartotras2, repartotras3, 
  
  # rename variables:
  setnames(ENCOUNTERS_all, old = "id", new = "person_id")
  
  ENCOUNTERS<-copy(ENCOUNTERS_all)[,.(encounter_id, encounter_src_id, person_id, encounter_start_date, encounter_end_date,encounter_derm_specialty,encounter_discharge_status,encounter_misc)] #
  
  fwrite(ENCOUNTERS, paste0(diroutput,"/ENCOUNTERS_",source,".csv"), quote = "auto")
  rm(ENCOUNTERS)
  
  ## CLINICAL_ITEMS
  #Action:  For each record: create a record of CLINICAL_ITEMS;  copy the values of ESENZIONI into CLINICAL_ITEMS according to the following table
  
  CLINICAL_ITEMS_<-copy(ENCOUNTERS_all)[,.(person_id,data_a,codcmp,codcm1,codcm2,codcm3,codcm4,codcm5,intproc,intsec1,intsec2,intsec3,intsec4,intsec5,encounter_id)]
  
  # rename variables:
  setnames(CLINICAL_ITEMS_, old = "data_a", new = "clin_item_date")
  setnames(CLINICAL_ITEMS_, old = "codcmp", new = "codcm0")
  setnames(CLINICAL_ITEMS_, old = "intproc", new = "intsec0")
  
  CLINICAL_ITEMS__<-melt(CLINICAL_ITEMS_, measure= patterns("^codcm","^intsec"), variable.name = "ord", value.name = c("dia1","dia2"))  [!(dia1=="" & dia2==""),] #dia1=diagnosis, dia2=procedure
  CLINICAL_ITEMS__<-CLINICAL_ITEMS__[,dia2:=as.character(dia2)]
  CLINICAL_ITEMS<-melt(CLINICAL_ITEMS__, measure= patterns("^dia"), variable.name = "clas", value.name = c("clin_item_code"))  
  
  # create others vars.
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,clin_item_src_id:=paste0(substr(source,22,nchar(source)),"_",encounter_id,"_",ord,"_",substr(clas,4,5))][, clin_item_setting:="1"]
  
  CLINICAL_ITEMS<-CLINICAL_ITEMS[clas=="dia2", clin_item_datatype:=4][clas=="dia1" & ord==1, clin_item_datatype:=1][clas=="dia1" & ord!=1,clin_item_datatype:=2]
  CLINICAL_ITEMS<-CLINICAL_ITEMS[clin_item_datatype==1 | clin_item_datatype==2,item_classification:='40'][clin_item_datatype==4, item_classification:='42']
  
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,clin_item_id:=1:.N]
  
  # keep only needed variables
  CLINICAL_ITEMS<-CLINICAL_ITEMS[,.(clin_item_id,clin_item_src_id,person_id,encounter_id,clin_item_code,item_classification,clin_item_datatype,clin_item_setting,clin_item_date)]
  
  fwrite(CLINICAL_ITEMS, paste0(diroutput,"/CLINICAL_ITEMS_",source,".csv"), quote = "auto")
  
  rm(CLINICAL_ITEMS_,CLINICAL_ITEMS__)
  rm(pippo)
  
  
}

rm(CLINICAL_ITEMS)



# 2.  Use SPECIALISTICA	to populate ENCOUNTERS, CLINICAL_ITEMS --OK! --------------
SPECIALISTICA_table<- files[str_detect(files,"^SPECIALISTICA")]

  
for (source in SPECIALISTICA_table){
  pippo <- fread(paste0(dirinput,source,".csv"))
  setkeyv(pippo,"id")
 
  
  ##ENCOUNTERS: 
  #Action:  For each record: create a record of ENCOUNTERS and label the records with a unique code stored in encounter id (primary key); copy the values of SPECIALISTICA into ENCOUNTERS according to the following table

  ENCOUNTERS_all<-copy(pippo)
  
  ENCOUNTERS_all<-ENCOUNTERS_all[,`:=`(encounter_id=1:.N, 
                                       encounter_src_id=paste0("SPECIALISTICA_", 1:.N),
                                       encounter_misc="99",
                                       encounter_discharge_status=99,
                                       encounter_start_date=dataprest,
                                       encounter_end_date=dataprest)]
  
  ENCOUNTERS_all<-ENCOUNTERS_all[codbranca %in% derm_specility_spa | codbranca  %in% derm_specility_spa,
                                 encounter_derm_specialty:=1][is.na(encounter_derm_specialty),
                                                              encounter_derm_specialty:=0]

  # rename variables:
  setnames(ENCOUNTERS_all, old = "id", new = "person_id")
  
  ENCOUNTERS<-copy(ENCOUNTERS_all)[,.(encounter_id, encounter_src_id, person_id, encounter_start_date, encounter_end_date,encounter_derm_specialty,  encounter_discharge_status,encounter_misc)] 
  
  fwrite(ENCOUNTERS, paste0(diroutput,"/ENCOUNTERS_",source,".csv"), quote = "auto")
  rm(ENCOUNTERS)
  
  ## CLINICAL_ITEMS
  #Action:  For each record: create a record of CLINICAL_ITEMS;  copy the values of ESENZIONI into CLINICAL_ITEMS according to the following table
  
  CLINICAL_ITEMS_<-copy(ENCOUNTERS_all)[,.(person_id,codprest,dataprest,encounter_id)]

  # rename variables:
  setnames(CLINICAL_ITEMS_, old = "codprest", new = "clin_item_code")
  setnames(CLINICAL_ITEMS_, old = "dataprest", new = "clin_item_date")
  
  # create others vars.
  CLINICAL_ITEMS_<-CLINICAL_ITEMS_[,`:=`(clin_item_id =1:.N,
                                         clin_item_src_id=paste0(encounter_id,"_",substr(source,19,nchar(source)),"_",1:.N),
                                         item_classification="41", 
                                         clin_item_datatype="4",
                                         clin_item_setting="6"  )]

  # keep only needed variables
  CLINICAL_ITEMS<-CLINICAL_ITEMS_[,.(clin_item_id,clin_item_src_id,person_id,encounter_id,clin_item_code,item_classification,clin_item_datatype,clin_item_setting,clin_item_date)]
  
  fwrite(CLINICAL_ITEMS, paste0(diroutput,"/CLINICAL_ITEMS_",source,".csv"), quote = "auto")
  
  
  rm(pippo, CLINICAL_ITEMS_)
  
  
}

rm(CLINICAL_ITEMS, ENCOUNTERS_all)


# 3.  Use PRONTO_SOCCORSO to populate CLINICAL_ITEMS, ENCOUNTERS --OK!------------------------------------------

PRONTO_SOCCORSO_table<- files[str_detect(files,"^PRONTO_SOCCORSO")]

for (source in PRONTO_SOCCORSO_table){
  pippo <- fread(paste0(dirinput,source,".csv"))
  setkeyv(pippo,"id")
  
  ##ENCOUNTERS: 
  #Action:  For each record: create a record of ENCOUNTERS and label the records with a unique code stored in encounter id (primary key); copy the values of PRONTO_SOCCORSO into ENCOUNTERS according to the following table
  ENCOUNTERS_all<-copy(pippo)
  
  ENCOUNTERS_all<-ENCOUNTERS_all[,`:=`(encounter_id=1:.N, 
                                       encounter_src_id=paste0("PRONTO_SOCCORSO_", 1:.N),
                                       encounter_misc="99",
                                       encounter_derm_specialty='88',
                                       encounter_start_date=data_a)]
  
  ENCOUNTERS_all<-ENCOUNTERS_all[,encounter_end_date:=min(data_d, end_study), by=c("encounter_id")]
  
  ENCOUNTERS_all<-ENCOUNTERS_all[(tipdim==0 | tipdim==1),
                                 encounter_discharge_status:=1][tipdim==2 ,
                                                                encounter_discharge_status:=2] [is.na(tipdim) |tipdim==99, 
                                                                                                encounter_discharge_status:=99]
  
  # rename variables:
  setnames(ENCOUNTERS_all, old = "id", new = "person_id")
  
  ENCOUNTERS<-copy(ENCOUNTERS_all)[,.(encounter_id, encounter_src_id, person_id, encounter_start_date, encounter_end_date, encounter_derm_specialty,encounter_discharge_status,encounter_misc)] 
  
  fwrite(ENCOUNTERS, paste0(diroutput,"/ENCOUNTERS_",source,".csv"), quote = "auto")
  rm(ENCOUNTERS)
  
  ## CLINICAL_ITEMS
  #Action:  For each record: create one record of CLINICAL_ITEMS for each non-empty cell among codcmp, codcm1,.., codcm5; copy the values of PRONTO_SOCCORSO into CLINICAL_ITEMS according to the following table
  if(DAP!="ARS"){

    CLINICAL_ITEMS_<-copy(ENCOUNTERS_all)[,.(person_id,data_a,codcmp,codcm1,codcm2,codcm3,codcm4,codcm5,encounter_id)]
    
    # rename variables:
    setnames(CLINICAL_ITEMS_, old = "data_a", new = "clin_item_date")
    setnames(CLINICAL_ITEMS_, old = "codcmp", new = "codcm0")

    CLINICAL_ITEMS__<-melt(CLINICAL_ITEMS_, measure= patterns("^codcm"), variable.name = "ord", value.name = c("dia"))  [dia1!="",] #dia1=diagnosis, dia2=procedure
    CLINICAL_ITEMS<-melt(CLINICAL_ITEMS__, measure= patterns("^dia"), variable.name = "clas", value.name = c("clin_item_code"))  
    
    # create others vars.
    CLINICAL_ITEMS<-CLINICAL_ITEMS[,clin_item_src_id:=paste0(substr(source,22,nchar(source)),"_",encounter_id,"_",ord,"_",substr(clas,4,5))]
    
    CLINICAL_ITEMS<-CLINICAL_ITEMS[ord==1, clin_item_datatype:=1][ord!=1, clin_item_datatype:=2]
    CLINICAL_ITEMS<-CLINICAL_ITEMS[,item_classification:='40'][, clin_item_setting:="3"]
    
    CLINICAL_ITEMS<-CLINICAL_ITEMS[,clin_item_id:=1:.N]
    
    # keep only needed variables
    CLINICAL_ITEMS<-CLINICAL_ITEMS[,.(clin_item_id,clin_item_src_id,person_id,encounter_id,clin_item_code,item_classification,clin_item_datatype,clin_item_setting,clin_item_date)]
    
    fwrite(CLINICAL_ITEMS, paste0(diroutput,"/CLINICAL_ITEMS_",source,".csv"), quote = "auto")
    
    rm(CLINICAL_ITEMS_,CLINICAL_ITEMS__)
    rm(pippo)
    
    
  } else {
    
    
    CLINICAL_ITEMS_<-copy(ENCOUNTERS_all)[,.(person_id,data_a,codcmp,encounter_id)]
    
    # rename variables:
    setnames(CLINICAL_ITEMS_, old = "data_a", new = "clin_item_date")
    setnames(CLINICAL_ITEMS_, old = "codcmp", new = "clin_item_code")
    
    # create others vars.
    CLINICAL_ITEMS_<-CLINICAL_ITEMS_[,clin_item_src_id:=paste0(substr(source,22,nchar(source)),"_",encounter_id,"_",1:.N)]
    
    CLINICAL_ITEMS_<-CLINICAL_ITEMS_[,clin_item_datatype:=1][,item_classification:='40'][, clin_item_setting:="3"]
    
    CLINICAL_ITEMS_<-CLINICAL_ITEMS_[,clin_item_id:=1:.N]
    
    # keep only needed variables
    CLINICAL_ITEMS<-CLINICAL_ITEMS_[,.(clin_item_id,clin_item_src_id,person_id,encounter_id,clin_item_code,item_classification,clin_item_datatype,clin_item_setting,clin_item_date)]
    
    fwrite(CLINICAL_ITEMS, paste0(diroutput,"/CLINICAL_ITEMS_",source,".csv"), quote = "auto")
    
    rm(CLINICAL_ITEMS_)
    rm(pippo)
  }
  
  
}

rm(CLINICAL_ITEMS)
# 4.  Use ESENZIONI to populate CLINICAL_ITEMS, ENCOUNTERS --OK! ------------------------------------------

ESENZIONI_table<- files[str_detect(files,"^ESENZIONI")]


for (source in ESENZIONI_table){
  pippo <- fread(paste0(dirinput,source,".csv"))
  setkeyv(pippo,"id")
  
  ## ENCOUNTERS:
  #Action: For each record: create a record of CLINICAL_ITEMS;  copy the values of ESENZIONI into CLINICAL_ITEMS according to the following table
  
  ENCOUNTERS_all<-copy(pippo)
  
  ENCOUNTERS_all<-ENCOUNTERS_all[,`:=`(encounter_id=1:.N, 
                                       encounter_src_id=paste0("ESENZIONI_", 1:.N),
                                       encounter_misc="99",
                                       encounter_discharge_status=99,
                                       encounter_derm_specialty='88',
                                       encounter_start_date=datai,
                                       encounter_end_date=dataf)]
  

  # rename variables:
  setnames(ENCOUNTERS_all, old = "id", new = "person_id")
  
  ENCOUNTERS<-copy(ENCOUNTERS_all)[,.(encounter_id, encounter_src_id, person_id, encounter_start_date, encounter_end_date, encounter_derm_specialty,encounter_discharge_status,encounter_misc)] 
  
  fwrite(ENCOUNTERS, paste0(diroutput,"/ENCOUNTERS_",source,".csv"), quote = "auto")
  rm(ENCOUNTERS)
  
  ## CLINICAL_ITEMS
  #Action:  For each record: create a record of CLINICAL_ITEMS;  copy the values of ESENZIONI into CLINICAL_ITEMS according to the following table
  
  CLINICAL_ITEMS_<-copy(ENCOUNTERS_all)[,.(person_id,ese_cod,datai,encounter_id)]
  
  # rename variables:
  setnames(CLINICAL_ITEMS_, old = "ese_cod", new = "clin_item_code")
  setnames(CLINICAL_ITEMS_, old = "datai", new = "clin_item_date")
  
  # create others vars.
  CLINICAL_ITEMS_<-CLINICAL_ITEMS_[,`:=`(clin_item_id =1:.N,
                                         clin_item_src_id=paste0(encounter_id,"_",substr(source,19,nchar(source)),"_",1:.N),
                                         item_classification="40", 
                                         clin_item_datatype="3",
                                         clin_item_setting="10"  )]
  
  # keep only needed variables
  CLINICAL_ITEMS<-CLINICAL_ITEMS_[,.(clin_item_id,clin_item_src_id,person_id,encounter_id,clin_item_code,item_classification,clin_item_datatype,clin_item_setting,clin_item_date)]
  
  fwrite(CLINICAL_ITEMS, paste0(diroutput,"/CLINICAL_ITEMS_",source,".csv"), quote = "auto")
  
  
  rm(pippo, CLINICAL_ITEMS_)
  
}

rm(CLINICAL_ITEMS)

# 5.  Use PRESCRIZIONI_FARMACI to populate DRUG_ITEMS --OK!  --------------------------------------------
PRESCRIZIONI_FARMACI_table <- files[str_detect(files,"^PRESCRIZIONI_FARMACI")]


for (source in PRESCRIZIONI_FARMACI_table){
  pippo <- fread(paste0(dirinput,source,".csv"), colClasses = list( character="id"))
  setkeyv(pippo,"id")
  
  ## DRUG_ITEMS
    #Action: For each record: create a record of DRUG_ITEMS; copy the values of PRESCRIZIONI_FARMACI into DRUG_ITEMS according to the following table
  DRUG_ITEMS<-copy(pippo)[,drug_item_id:=1:.N]
  
  # rename variables:
  setnames(DRUG_ITEMS, old = "id", new = "person_id")
  setnames(DRUG_ITEMS, old = "datasped", new = "drug_item_date")
  setnames(DRUG_ITEMS, old = "atc", new = "drug_item_code")
  setnames(DRUG_ITEMS, old = "aic", new = "drug_item_npc")
  setnames(DRUG_ITEMS, old = "pezzi", new = "drug_item_quantity")
  
  DRUG_ITEMS<-DRUG_ITEMS[,`:=`(drug_item_src_id="",encounter_id="", item_classification=1,drug_item_datatype=1,drug_item_dosage_available=0,drug_item_presc_specialty="",drug_item_quantity_unit="",drug_item_presc_daily_dose="",drug_item_days_supply="",drug_item_strength="",drug_item_strength_unit="",drug_item_adm_route="",drug_item_dose_form="")] #drug_item_total_ddd=ddd 
  #drug_item_setting=‘4’ if from community pharmacy (irrespective of which organization purchased the drug) 
                    #‘5’ if from hospital pharmacy
  
  
  # keep only needed variables
  DRUG_ITEMS<-DRUG_ITEMS[,.(drug_item_id,drug_item_src_id,person_id,encounter_id,drug_item_code,item_classification,drug_item_datatype,drug_item_npc,drug_item_presc_specialty,drug_item_date,drug_item_dosage_available,drug_item_quantity,drug_item_quantity_unit,drug_item_presc_daily_dose,drug_item_days_supply,drug_item_strength,drug_item_strength_unit,drug_item_adm_route,drug_item_dose_form)] #,drug_item_total_ddd, ,drug_item_setting
  
  
  fwrite(DRUG_ITEMS, paste0(diroutput,"/DRUG_ITEMS_",substr(source,22,29),".csv"), quote = "auto")
  rm(pippo, DRUG_ITEMS)
}


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

#create person_id as number that identifier uniquely person, used to link across tables. (Primary key)
PERSONS<-copy(ANAFULL)

#renamed vars
setnames(PERSONS,"ID","person_id_src")
setnames(PERSONS,"DATA_NASCITA","birth_date")
setnames(PERSONS,"DATA_MORTE_MARSI","death_date")
setnames(PERSONS,"SESSO","sex")

# make the data unique
PERSONS<-unique(PERSONS[,.(person_id_src,birth_date,sex,death_date)])

# keep only needed vars.
PERSONS<-PERSONS[,person_id:=1:.N]
PERSONS<-PERSONS[,.(person_id,person_id_src,birth_date,sex,death_date)]

fwrite(PERSONS, paste0(diroutput,"/PERSONS.csv"), quote = "auto")


## OBSERVATION_PERIODS: 
  # Action: one row of ANAGRAFE_ASSISTITI generates one row of OBSERVATION_PERIODS (multiple observations per person are possible)

OBSERVATION_PERIODS<-copy(ANAGRAFE_ASSISTITI)[,source:="ALL"][,obs_period_id:=1:.N]

#usa CreateSpells prima di fare OBSERVATION_PERIODS

#renamed vars
setnames(OBSERVATION_PERIODS,"id","person_id")
setnames(OBSERVATION_PERIODS,"data_inizioass","obs_period_start_date")
setnames(OBSERVATION_PERIODS,"data_fineass","obs_period_end_date")
setnames(OBSERVATION_PERIODS,"datanas","birth_date")
setnames(OBSERVATION_PERIODS,"datadec","death_date")
setnames(OBSERVATION_PERIODS,"sesso","sex")

# create obs_period_end_reason
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[obs_period_end_date==date_end, obs_period_end_reason:=1]
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[obs_period_end_date==death_date, obs_period_end_reason:=2]
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[!is.na(death_date) & (obs_period_end_date!=death_date & obs_period_end_date!=date_end), obs_period_end_reason:=3] 

# keep only needed vars.
OBSERVATION_PERIODS<-OBSERVATION_PERIODS[,.(obs_period_id,person_id,source,obs_period_start_date,obs_period_end_date,obs_period_end_reason)]

fwrite(OBSERVATION_PERIODS, paste0(diroutput,"/OBSERVATION_PERIODS.csv"), quote = "auto")

rm(ANAGRAFE_ASSISTITI, PERSONS, OBSERVATION_PERIODS)




