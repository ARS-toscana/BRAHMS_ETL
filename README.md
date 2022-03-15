# BRAHMS_ETL
ETL dal CDM TheShinISS all'CDM BRAHMS 

## Istruzioni

All'interno di questa repository trovate lo script R (**_01-07_ETL_Brahms_v2.R_**) che trasforma i dati al CDM Brahms.
<br>(la versione precedente _01-07_ETL_Brahms_v1.R_ è nella cartella old)

Lo script richiama inizialmente un file di parametri (_parameters_toBrahms_v2.R_) all'interno del quale:
 - si fissa il nome del DAP (riga 4), che sarà necessario cambiare con l'estrazione reale 
 - si richiamano le librerie necessarie (9-16)
 - si fissano le directory (19-42), che sarà adattare cambiare per l'estrazione reale
 - si fissano i parametri per le date (44-46)
 - si definiscono i codici per le specialità (60-70)



Il programma è diviso in 7 sezioni: 
 - le prime 6 elaborano ognuna della tabelle di TheShinISS CDM (_RICOVERI_OSPEDALIERI_, _SPECIALISTICA_, _PRONTO_SOCCORSO_, _ESENZIONI_, _PRESCRIZIONI_FARMACI_ e _ANANGRAFE_ASSISTITI_.) al Brahms CDM;
 - la settima 'appende' tutti i file delle stesso CDM tables per crearne una unica.




**Per l'esecuzione di prova NON è NECESSARIO FARE NESSUNA MODIFICA**, si esegue tutto lo script _01-07_ETL_Brahms_v2.R_ e si trovano le tabelle Brahms CDM nella cartella **CDMtables**



Per qualsiasi info: claudia.bartolini@ars.toscana.it



