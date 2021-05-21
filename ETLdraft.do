local ore=0
local millisec=1000*60*60*`ore'
sleep `millisec'

set more off
global dirpar "./"

do ${dirpar}par.do
// 


dsconcat ${dirsql}FED.dta ${dirsql}SPF2019.dta
rename DATAERO datasped
rename PEZZI_ARSNEW pezzi
gen aic=substr( CODFARM,2,.)
format datasped %tdCCYY-NN-DD
keep id aic datasped pezzi 
order id aic datasped pezzi 
outsheet using "${diroutput}prescrizioni.csv",replace noquote comma nolabel 


use ${dirsql}SDO,clear
rename DATAMM data_a 
rename DATDIM data_d
rename DIADIM codcmp
rename CODCHI intprinc
forvalues j=1/5{
	local jp=`j'+1
	rename PAT`j' codcm`j'
	rename CODCHI`jp' intsec`j'
	} 
format data_a data_d %tdCCYY-NN-DD
keep id data_a data_d codcmp codcm* intprinc intsec*
order id data_a data_d codcmp codcm* intprinc intsec*
outsheet using "${diroutput}sdo.csv",replace noquote comma nolabel 



use ${dirsql}EXE,clear
rename RILASCIO datai
rename SCADENZA dataf
replace dataf=mdy(12,31,9999) if mi(dataf)
rename GRUPPO ese_cod
format data* %tdCCYY-NN-DD
keep id datai dataf ese_cod
order id datai dataf ese_cod 
outsheet using "${diroutput}ese.csv",replace noquote comma nolabel 


use ${dirsql}VCN,clear
rename ID_UNIVERSALE id
rename DATA_SOMMINISTRAZIONE datavac
rename COD_PREST_VACCINO aic
gen catrisc=""
gen conrisc=""
keep id datavac aic catrisc conrisc vacpre
rename id IDUNI
merge m:1 IDUNI using ${dirsql}ANA,keep(match) keepus(SESSO DATA_NASCITA)
drop _merge
rename IDUNI id
gen sesso = "M" if SESSO ==  "1"
replace sesso = "F" if SESSO == "2"
drop SESSO
rename DATA_NASCITA datanas
format data* %tdCCYY-NN-DD
outsheet using "${diroutput}ANAGRAFE VACCINALE.csv",replace noquote comma nolabel


use ${dirsql}ANA,clear
rename IDUNI id
gen sesso = "M" if SESSO ==  "1"
replace sesso = "F" if SESSO == "2"
drop SESSO
rename DATA_NASCITA datanas
rename DATA_M datadec
rename INIZIO dataini
rename FINE datafin
format data* %tdCCYY-NN-DD
outsheet using "${diroutput}ANAGRAFE ASSISTITI.csv",replace noquote comma nolabel

/*
gen catrisc = "A" if strmatch(Rischio,"*Soggetti 65enni*") |strmatch(Rischio,"*pari o superiore a 65 anni*")
replace catrisc = "D" if strmatch(Rischio,"*ravidanz*")
replace catrisc = "Jj" if strmatch(Rischio,"*onator*")
replace catrisc = "H" if strmatch(Rischio,"Addett*") | strmatch(Rischio,"Altre categorie socialmente utili*") | strmatch(Rischio,"*Macellat*")
count if mi(catrisc)
A Soggetti di et‡ pari o superiore a 65 anni
B Soggetti di et‡ compresa fra 6 mesi e 65 anni con condizioni di rischio
C Bambini e adolescenti in trattamento a lungo termine con acido acetilsalicilico
D Donne nel secondo e terzo trimestre di gravidanza
E Individui di qualunque et‡ ricoverati presso strutture per lungodegenti
F Medici e personale sanitario di assistenza
G Familiari e contatti di soggetti ad alto rischio
H Soggetti addetti a servizi pubblici di primario interesse collettivo e categorie di lavoratori
I Personale che, per motivi di lavoro, Ë a contatto con animali che potrebbero costituire fonte di infezione da virus influenzali non umani
J Donatori di Sangue
K Popolazione generale (escluse le categorie di cui sopra)


`"Addetti ai servizi cimiteriali e funebri"'
 `"Addetti ai servizi di raccolta, trasporto e smaltimento dei rifiuti"' 
 `"Addetti al lavaggio di materiali potenzialmente infetti"' 
 `"Addetti al soccorso e al trasporto di infortunati e infermi"' 
 `"Addetti al trasporto di animali vivi"' 
 `"Addetti all'attivit√† di allevamento"' 
 `"Alcolismo cronico"' 
 `"Altre categorie socialmente utili che potrebbero avvantaggiarsi della vaccinazione per motivi vincolanti allo svolgimento della loro attivita'¬† lavorativa (FORZE ARMATE, POLIZIA M UNICIPALE, PERSONALE DELLE PROTEZIONE CIVILE, ADDETTI POSTE E TELECOMUNICAZ IONI, VOLONTARI SERVIZI SANITARI DI EMERGENZA , PERSONALE DI ASSISTENZA CAS E DI RIPOSO, PERSONALE DEGLI ASILI NIDO E SCUOLE DI OGNI ORDINE E GRADO)"' 
`"Altre cause di asplenia"' 
`"Asplenia anatomica o funzionale"'
`"Asplenia anatomica o funzionale o candidati alla splenectomia"' 
`"Asplenia post-trau matica"' 
`"Candidati alla splenectomia"' 
`"Cardiopatie croniche"' 
`"Cirrosi epatica, epatopatie croniche evolutive"' 
`"Condizioni associate a immunodepressione (come trapianto d‚??organo o terapia antineoplastica, compresa la terapia sistemica corticosteroidea ad alte dosi)"' 
`"Contatti di soggetti affetti"' 
`"Conviventi di soggetti a rischio"' 
`"Conviventi, in particolare bambini non compresi nelle categorie indicate all'art. 1 legge n. 165/1991 , e altre persone a contatto con soggetti hbsag positivi;"' 
`"Conviventi/co ntatti di casi di morbillo, suscettibili e anamnesticamente negativi possib ilmente entro 72 ore dall'esposizione"' 
`"Conviventi/contatti di casi di va ricella,  anamnesticamente negativi possibilmente entro 72 ore dall'esposiz ione"' `"Deficienza dei fattori terminali del complemento"' 
`"Deficienza te rminale del complemento- leucemie, linfomi, mieloma multiplo"' 
`"Diabete me llito"' 
`"Diabete mellito e altre malattie metaboliche (inclusi gli obesi c on bmi>30 e gravi patologie concomitanti)"' 
`"Diabete mellito, in particola re se in difficile compenso "' 
`"Donatori di sangue"' 
`"Donatori di sangue appartenenti a gruppi sanguigni rari"' 
`"Donne che all‚??inizio della stagi one epidemica si trovino nel secondo e terzo trimestre di gravidanza"' 
`"Do nne operate per lesioni cervicali dovute ad infezioni da HPV"' 
`"Emodializz ati e uremici cronici per i quali si prevede l‚??entrata in dialisi"' 
`"Epatopatia cronica"' 
`"Familiari e contatti (ADULTI E BAMBINI) di soggetti ad alto rischio DI COMPLICANZE (INDIPENDENTEMENTE DEL FATTO CHE IL SOGGETTO A RISCHIO SIA STATO E MENO VACCINATO)"' 
`"Forze di polizia"' 
`"Gravidanza"' 
`"Immunodeficienza acquisita"' 
`"Immunodeficienze congenite"' 
`"Immunodefici enze congenite o acquisite (es. deficit di igg2, deficit di complemento, im munosoppressione da chemioterapia, hiv positivi)"' 
`"Immunosoppressione iat rogena clinicamente significativa"' 
`"Individui di qualsiasi et√† ricoverat i presso strutture per lungodegenti"' 
`"Infine √® pratica internazionalment e diffusa l‚??offerta attiva e gratuita della vaccinazione antinfluenzale d a parte dei datori di lavoro ai lavoratori particolarmente esposti per atti vit√† svolta e al fine di contenere ricadute negative sulla produttivit√†." ' 
`"Insufficienza renale / surrenale cronica"' 
`"Insufficienza renale / sur renale cronica, sindrome nefrosica, dializzati o candidati alla dialisi"' 
`"Insufficienza renale cronica con creatinina clearance <30 ml/min"' 
`"Lavor atori Agricoli"' 
`"Lavoratori Del Legno"'
 `"Macellatori e vaccinatori"' 
 `"M alattie congenite o acquisite che comportino  CARENTE PRODUZIONE DI ANTICOR PI, immunosoppressione indotta da farmaci o da hiv"' 
 `"Malattie croniche a carico dell‚??apparato respiratorio (inclusa l‚??asma grave, la displasia b roncopolmonare, la fibrosi cistica e la broncopneumopatia cronico ostruttiv a (bpco)"' 
 `"Malattie degli organi emopoietici ed emoglobinopatie"' 
 `"Malat tie dell‚??apparato cardio-circolatorio, comprese le cardiopatie congenite e acquisite"' 
 `"Malattie infiammatorie croniche e sindromi da malassorbimen to intestinali"' 
 `"Malattie polmonari croniche"' 
 `"Marittimi E Lavoratori P ortuali"' 
 `"Medici e personale sanitario di assistenza IN STRUTTURE CHE, AT TRAVERSO LA LORO ATTIVITA', SONO IN GRADO DI TRASMETTERE L'INFLUENZA A CHI E' AD ALTO RISCHIO DI COMPLICANZE INFLUENZALI"' 
 `"Metallurgici E Metalmecca nici"' 
 `"Neoplasie diffuse"'
  `"Non definita"' 
  `"Operai Addetti Alla Manipol azione Delle Immondizie"' 
  `"Operai E Manovali Addetti All'Edilizia"'
   `"Oper ai E Manovali Delle Ferrovie"' 
   `"Operatori sanitari suscettibili"' 
   `"Patolo gia cardiovascolare "' 
   `"Patologie associate a un aumentato rischio di aspi razione delle secrezioni respiratorie (ad es. malattie neuromuscolari)"'
    `" Patologie onco - ematologiche Leucemie, linfomi, mieloma multiplo"' 
    `"Patol ogie per le quali sono programmati importanti interventi chirurgici"' 
 `"Pat ologie richiedenti un trattamento immunosoppressivo a lungo termine"' 
 `"Per dita di fluidi cerebrospinali"' 
 `"Personale addetto alla lavorazione degli emoderivati;"' 
 `"Personale e ospiti di istituti per portatori di handicap f isici e mentali;"' 
 `"Personale ssn neoassunto o in attivit√† a maggior risc hio di contagio e/o che lavori in reparti a richio"' 
 `"Persone che si rechi no all'estero, per motivi di lavoro, in aree geografiche ad alta endemia di hbv;"' 
`"Persone suscettibili che lavorano in ambiente sanitario soprattut to se √® a contatto con neonati, bambini, donne gravide o con persone immun odepresse."' 
`"Polizia, carabinieri, guardia di finanza, agenti di custodia , vigili del fuoco e vigili urbani"' 
`"Portatori di impianto cocleare"' 
`"P roblemi Respiratori Lievi"' 
`"Puerpere e donne che effettuano un‚??interruz ione di gravidanza senza evidenza sierologica di immunit√† o documentata va ccinazione"' 
`"Riceventi fattori della coagulazione concentrati"' 
`"Situazi one epidemiologica ad alto rischio su valutazione dell‚?? isp della asl"' 
` "Soggetti 65enni a partire dalla coorte di nascita 1950"' 
`"Soggetti 65enni a partire dalla coorte di nascita 1952"' 
`"Soggetti addetti a servizi di p rimario interesse collettivo e categorie di lavoratori"' 
`"Soggetti che svo lgono attivit√† di lavoro, studio e volontariato nel settore della sanit√†; "' 
`"Soggetti con epatopatie croniche in particolare hcv correlata (l‚??inf ezione da hbv potrebbe causare l‚??aggravamento dell‚??epatopatia)"' 
`"Sogg etti con infezione da HIV"' 
`"Soggetti destinati a terapia immunosoppressiv a"' 
`"Soggetti di et√† pari o superiore a 65 anni"' 
`"Soggetti in attesa di trapianto di organo solido"'
 `"Soggetti ospiti di comunit√†"'
  `"Soggetto c on vaccinazione di propria iniziativa e a proprie spese - (Causale generica per MMG in caso di somministrazione richiesta dal proprio assistito)"' 
  `"S portivi Affiliati Federazioni Coni"' 
  `"Straccivendoli"' 
  `"Tatuatori e bodyp ierciers"' 
  `"Terapia sistemica con elevate quantit√† di corticosteroidi"' 
  ` "Tossicodipendenti"' 
 `"Trapiantati o candidati al trapianto"' 
 `"Trapianto d ‚??organo o di midollo"' 
 `"Trattamento Post-Esposizione"' 
 `"Tumori"' 
 `"Uomi ni che fanno sesso con uomini"' 
 `"Veterinari pubblici e libero-professionis ti"' 
 `"Viaggiatori Internazionali"' 
 `"Vigili del fuoco"' 
 `"Zoster recidivan te"' 
 `"broncopneumopatia cronico ostruttiva (BPCO)"'
