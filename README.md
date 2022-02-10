# BRAHMS_ETL
ETL dal CDM TheShinISS all'CDM BRAHMS 

## Istruzioni

All'interno di questa repository trovate lo script R (_01-07_ETL_Brahms_v1.R_) che trasforma i dati al CDM Brahms.

Lo script richiama inizialmente un file di parametri (_parameters_toBrahms.R_) all'interno del quale:
 - si fissa il nome del DAP (riga 4), che sarà necessario cambiare con l'estrazione reale 
 - si richiamano le librerie necessarie (9-16)
 - si fissano le directory (19-39), che sarà adattare cambiare per l'estrazione reale
 - si fissano i parametri per le date (42-44)
 - si definiscono i codici per le specialità (52-62)



Il programma è diviso in 7 sezioni: 
 - le prime 6 elaborano ognuna della tebelle di TheShinISS CDM (_RICOVERI_OSPEDALIERI_, _SPECIALISTICA_, _PRONTO_SOCCORSO_, _ESENZIONI_, _PRESCRIZIONI_FARMACI_ e _ANANGRAFE_ASSISTITI_.) al Brahms CDM;
 - la settima 'appende' tutti i file delle stesso CDM tables per crearne una unica.




**Per l'esecuzione di prova NON è NECESSARIO FARE NESSUNA MODIFICA**, si esegue tutto lo script _01-07_ETL_Brahms_v1.R_ e si trovano le tabelle Brahms CDM nella cartella **20220210_BRAHMS_CDM**



Per qualsiasi info: claudia.bartolini@ars.toscana.it



