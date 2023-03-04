/*
 Oltre le query presenti di seguito sono stati effettuati dei
 cambi di TIPO per molte colonne prese in considerazione nello
 studio e dei cambi di FORMATO dei file ricevuti,
 direttamente con lo strumento utilizzato nell'analisi
 e quindi non tracciato da query SQL.
 */

-- Ultimi aggiornamenti al 20/02/2023
-- TABELLA: NDOM_Avvisi_Contratti_Attivi_20230131_tdoc
update NDOM_Avvisi_Contratti_Attivi_20230131_tdoc
set IMPORTO_PAREGGIO = replace(IMPORTO_PAREGGIO, ',','.'),
    IMPORTO_CONTRATTO = replace(IMPORTO_CONTRATTO, ',','.');

-- TABELLA: DOM_Avvisi_Contratti_Attivi_20230131_tdoc
update DOM_Avvisi_Contratti_Attivi_20230131_tdoc
set IMPORTO_PAREGGIO = replace(IMPORTO_PAREGGIO, ',','.'),
    IMPORTO_CONTRATTO = replace(IMPORTO_CONTRATTO, ',','.');

-- Aggiornamenti al 17/02/2023
-- TABELLA: NDOM_Avvisi_Contratti_Attivi_20230131
update NDOM_Avvisi_Contratti_Attivi_20230131
set IMPORTO_PAREGGIO = replace(IMPORTO_PAREGGIO, ',','.'),
    IMPORTO_CONTRATTO = replace(IMPORTO_CONTRATTO, ',','.');

-- TABELLA: DOM_Avvisi_Contratti_Attivi_20230131
update DOM_Avvisi_Contratti_Attivi_20230131
set IMPORTO_PAREGGIO = replace(IMPORTO_PAREGGIO, ',','.'),
    IMPORTO_CONTRATTO = replace(IMPORTO_CONTRATTO, ',','.');

-- TABELLA: 1_elenco_pdf_2022
alter table "1_elenco_pdf_2022"
add DATA text;
update "1_elenco_pdf_2022"
set
    DATA = SUBSTRING(ORARIO,7,4) || SUBSTRING(ORARIO,4,2) || SUBSTRING(ORARIO,1,2)
WHERE ORARIO IS NOT NULL;
alter table "1_elenco_pdf_2022"
DROP ORARIO;

-- TABELLA: QB20230019208-ESENZIONI_RICEVUTE_2022___STATO_1
alter table "QB20230019208-ESENZIONI_RICEVUTE_2022___STATO_1"
add DATADENUNCIA;
alter table "QB20230019208-ESENZIONI_RICEVUTE_2022___STATO_1"
add DATASTATO;
update "QB20230019208-ESENZIONI_RICEVUTE_2022___STATO_1"
set DATADENUNCIA = SUBSTRING(DATA_DENUNCIA,7,10) || SUBSTRING(DATA_DENUNCIA,4,2) || SUBSTRING(DATA_DENUNCIA,0,3)
WHERE DATA_DENUNCIA is not NULL;
UPDATE "QB20230019208-ESENZIONI_RICEVUTE_2022___STATO_1"
SET DATA_STATO = substring(DATA_STATO, 0,11);
update "QB20230019208-ESENZIONI_RICEVUTE_2022___STATO_1"
set DATASTATO = SUBSTRING(DATA_STATO,7,10)  || SUBSTRING(DATA_STATO,4,2) || SUBSTRING(DATA_STATO,0,3)
WHERE DATA_STATO is not NULL;
alter table "QB20230019208-ESENZIONI_RICEVUTE_2022___STATO_1"
DROP DATA_DENUNCIA;
alter table "QB20230019208-ESENZIONI_RICEVUTE_2022___STATO_1"
DROP DATA_STATO;

-- TABELLA: Esiti PEC 2022
alter table "Esiti PEC 2022"
drop ISCRIVIBILE_RUOLO;
alter table "Esiti PEC 2022"
drop CODICE_SCATOLA;
alter table "Esiti PEC 2022"
drop FLAG_CONF_INDIC_DATA_FESTIVA;
alter table "Esiti PEC 2022"
drop FLAG_IND_DATA_RILEV_TIMBRO;

-- TABELLA: 8_pagamenti_doc_20220131_con_importo
update "8_pagamenti_doc_20220131_con_importo"
set IMPORTO_PAREGGIO = replace(IMPORTO_PAREGGIO,',','.');

-- TABELLA: Rateizzazioni_TARI_2022_v2
UPDATE Rateizzazioni_TARI_2022_v2
    SET DATA_CHIUSURA = substring(DATA_CHIUSURA, 0,5) || substring(DATA_CHIUSURA, 7,2) ||substring(DATA_CHIUSURA, 5,2)
WHERE DATA_CHIUSURA IS NOT NULL;
UPDATE Rateizzazioni_TARI_2022_v2
    SET DATA_MODIFICA = substring(DATA_MODIFICA, 0,5) || substring(DATA_MODIFICA, 7,2) ||substring(DATA_MODIFICA, 5,2)
WHERE DATA_MODIFICA IS NOT NULL;
UPDATE Rateizzazioni_TARI_2022_v2
    SET DATA_CREAZIONE = substring(DATA_CREAZIONE, 0,5) || substring(DATA_CREAZIONE, 7,2) ||substring(DATA_CREAZIONE, 5,2)
WHERE DATA_CREAZIONE IS NOT NULL;


alter table Rateizzazioni_TARI_2022_v2
add DATA_CREAZIONE text;
alter table Rateizzazioni_TARI_2022_v2
add DATA_MODIFICA text;
alter table Rateizzazioni_TARI_2022_v2
add DATA_CHIUSURA text;
UPDATE Rateizzazioni_TARI_2022_v2
SET "Data Creazione" = substring("Data Creazione", 0,11)
WHERE "Data Creazione" is not NULL;
UPDATE Rateizzazioni_TARI_2022_v2
SET "Data Modifica" = substring("Data Modifica", 0,11)
WHERE "Data Creazione" is not NULL;
UPDATE Rateizzazioni_TARI_2022_v2
SET DATA_CREAZIONE = SUBSTRING("Data Creazione",7,10) || SUBSTRING("Data Creazione",0,3) || SUBSTRING("Data Creazione",4,2)
WHERE "Data Creazione" is not NULL;
UPDATE Rateizzazioni_TARI_2022_v2
SET DATA_MODIFICA = SUBSTRING("Data Modifica",7,10) || SUBSTRING("Data Modifica",0,3) || SUBSTRING("Data Modifica",4,2)
WHERE "Data Modifica" is not NULL;
UPDATE Rateizzazioni_TARI_2022_v2
SET DATA_CHIUSURA = SUBSTRING("Data Chiusura ",7,10) || SUBSTRING("Data Chiusura ",0,3) || SUBSTRING("Data Chiusura ",4,2)
WHERE "Data Chiusura " is not NULL;
alter table Rateizzazioni_TARI_2022_v2
drop "Data Creazione";
alter table Rateizzazioni_TARI_2022_v2
drop "Data Modifica";
alter table Rateizzazioni_TARI_2022_v2
drop "Data Chiusura ";

-- TABELLA: 5b_rateizzazioni_concesse_fatt_2022
update "5b_rateizzazioni_concesse_fatt_2022"
set IMPORTO_RATEIZZATO = replace(IMPORTO_RATEIZZATO, ',','.');

-- TABELLA: 3_PRATICHE_INGRESSO
update "3_PRATICHE_INGRESSO"
    set DataCreazione = substring(DataCreazione,7,4) || substring(DataCreazione,4,2) || substring(DataCreazione,1,2)
where DataCreazione is not null;
update "3_PRATICHE_INGRESSO"
    set DataModifica = substring(DataModifica,7,4) || substring(DataModifica,4,2) || substring(DataModifica,1,2)
where DataModifica is not null;
update "3_PRATICHE_INGRESSO"
    set DataChiusura = substring(DataChiusura,7,4) || substring(DataChiusura,4,2) || substring(DataChiusura,1,2)
where DataChiusura is not null;

-- TABELLA: 4_PRATICHE_INGRESSO_3_MESI_DOPO_CICLO_1
update "4_PRATICHE_INGRESSO_3_MESI_DOPO_CICLO_1"
    set DataCreazione = substring(DataCreazione,7,4) || substring(DataCreazione,4,2) || substring(DataCreazione,1,2)
where DataCreazione is not null;
update "4_PRATICHE_INGRESSO_3_MESI_DOPO_CICLO_1"
    set DataModifica = substring(DataModifica,7,4) || substring(DataModifica,4,2) || substring(DataModifica,1,2)
where DataModifica is not null;
update "4_PRATICHE_INGRESSO_3_MESI_DOPO_CICLO_1"
    set DataChiusura = substring(DataChiusura,7,4) || substring(DataChiusura,4,2) || substring(DataChiusura,1,2)
where DataChiusura is not null;

-- TABELLA: 4_PRATICHE_INGRESSO_3_MESI_DOPO_CICLO_2
update "4_PRATICHE_INGRESSO_3_MESI_DOPO_CICLO_2"
    set DataCreazione = substring(DataCreazione,7,4) || substring(DataCreazione,4,2) || substring(DataCreazione,1,2)
where DataCreazione is not null;
update "4_PRATICHE_INGRESSO_3_MESI_DOPO_CICLO_2"
    set DataModifica = substring(DataModifica,7,4) || substring(DataModifica,4,2) || substring(DataModifica,1,2)
where DataModifica is not null;
update "4_PRATICHE_INGRESSO_3_MESI_DOPO_CICLO_2"
    set DataChiusura = substring(DataChiusura,7,4) || substring(DataChiusura,4,2) || substring(DataChiusura,1,2)
where DataChiusura is not null;

-- TABELLA: RiconciliatiPoste
-- 1. Eliminate alcune colonne superflue con informazioni ridondanti
alter table RiconciliatiPoste
drop TIPOIDCANALEPAGAMENTO;alter table RiconciliatiPoste
drop DATADIACCREDITO;alter table RiconciliatiPoste
drop TIPORICONCILIAZIONE;alter table RiconciliatiPoste
drop DATARICONCILIAZIONE;alter table RiconciliatiPoste
drop RIFERIMENTIDIRICONCILIAZIONE;alter table RiconciliatiPoste
drop IDENTIFICATIVOFLUSSONODO;alter table RiconciliatiPoste
drop IBANACCREDITO;alter table RiconciliatiPoste
drop DOMINIOTITOLARE;alter table RiconciliatiPoste
drop INDICEQUOTA;alter table RiconciliatiPoste
drop field42;
-- 2. Aggiunte nuove colonne
alter table RiconciliatiPoste
add DATA_OPERAZIONE;
alter table RiconciliatiPoste
add DATA_PAGAMENTO;
UPDATE RiconciliatiPoste
SET DATA_PAGAMENTO = '20' || SUBSTRING(DATAPAGAMENTO,7,9) || SUBSTRING(DATAPAGAMENTO,4,2) || SUBSTRING(DATAPAGAMENTO,0,3)
WHERE DATAPAGAMENTO is not NULL;

-- TABELLA: RiconciliatiPoste_novembre
-- 1. Rimozione colonne superflue con informazioni ridondanti o non utili all'analisi
alter table RiconciliatiPoste_novembre
drop "TIPO ID CANALE PAGAMENTO";alter table RiconciliatiPoste_novembre
drop "DATA DI ACCREDITO";alter table RiconciliatiPoste_novembre
drop "TIPO RICONCILIAZIONE";alter table RiconciliatiPoste_novembre
drop "DATA RICONCILIAZIONE";alter table RiconciliatiPoste_novembre
drop "RIFERIMENTI DI RICONCILIAZIONE";alter table RiconciliatiPoste_novembre
drop "IDENTIFICATIVO FLUSSO NODO         ";alter table RiconciliatiPoste_novembre
drop "IBAN ACCREDITO";alter table RiconciliatiPoste_novembre
drop "DOMINIO TITOLARE";alter table RiconciliatiPoste_novembre
drop "INDICE QUOTA";
-- 2. Rinominate le colonne in modo da essere congruenti con la tabella RiconciliatiPoste
ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "COD.FISCALE ENTE/PA" TO "COD.FISCALEENTE/PA";ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "NOME ENTE/PA" TO "NOMEENTE/PA";ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "NOME DEL SERVIZIO" TO NOMEDELSERVIZIO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "DATA OPERAZIONE" TO DATAOPERAZIONE;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "TIPO RIF. CREDITORE" TO "TIPORIF.CREDITORE";ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "CODICE RIF. CREDITORE" TO "CODICERIF.CREDITORE";ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "TIPO AVVISO" TO TIPOAVVISO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "CODICE AVVISO" TO CODICEAVVISO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "TIPO RATA" TO TIPORATA;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "CODICE RATA" TO CODICERATA;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "DATA SCADENZA" TO DATASCADENZA;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "IMPORTO AVVISO" TO IMPORTOAVVISO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "TIPO DATI PA" TO TIPODATIPA;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "DATI PA" TO DATIPA;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "ID VERSANTE" TO IDVERSANTE;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "ANAGRAFICA VERSANTE" TO ANAGRAFICAVERSANTE;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "DATA PAGAMENTO" TO DATAPAGAMENTO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "RETE INCASSO" TO RETEINCASSO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "IDENTIFICATIVO PSP" TO IDENTIFICATIVOPSP;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "CODICE ID CANALE PAGAMENTO" TO CODICEIDCANALEPAGAMENTO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "TIPO PAGAMENTO ESEGUITO" TO TIPOPAGAMENTOESEGUITO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "IMPORTO PAGATO" TO IMPORTOPAGATO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "IMPORTO TOTALE ACCREDITATO" TO IMPORTOTOTALEACCREDITATO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "DOMINIO BENEFICIARIO" TO DOMINIOBENEFICIARIO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "DENOMINAZIONE BENEFICIARIO" TO DENOMINAZIONEBENEFICIARIO;ALTER TABLE RiconciliatiPoste_novembre
RENAME COLUMN "QUOTA SECONDARIA" TO QUOTASECONDARIA;
-- 3. Aggiunte nuove colonne
alter table RiconciliatiPoste_novembre
add DATA_OPERAZIONE;
alter table RiconciliatiPoste_novembre
add DATA_PAGAMENTO;
UPDATE RiconciliatiPoste_novembre
SET DATA_OPERAZIONE = '2' || SUBSTRING(DATAOPERAZIONE,7,10) || SUBSTRING(DATAOPERAZIONE,0,3) || SUBSTRING(DATAOPERAZIONE,4,2)
WHERE DATAOPERAZIONE is not NULL;
UPDATE RiconciliatiPoste_novembre
SET DATA_OPERAZIONE = '2' || SUBSTRING(DATAOPERAZIONE,7,10) || SUBSTRING(DATAOPERAZIONE,0,3) || '0' || SUBSTRING(DATAOPERAZIONE,4,1)
WHERE DATAOPERAZIONE in ('11/2/2022', '11/3/2022','11/4/2022','11/7/2022','11/8/2022','11/9/2022');
UPDATE RiconciliatiPoste_novembre
SET DATA_OPERAZIONE = SUBSTRING(DATA_OPERAZIONE,2,length(DATA_OPERAZIONE))
    where DATA_OPERAZIONE in (select distinct(DATA_OPERAZIONE)
                              from RiconciliatiPoste_novembre
                              WHERE DATA_OPERAZIONE like '22%');
UPDATE RiconciliatiPoste_novembre
SET DATA_PAGAMENTO = SUBSTRING(DATAPAGAMENTO,7,10) || SUBSTRING(DATAPAGAMENTO,0,3) || SUBSTRING(DATAPAGAMENTO,4,2)
WHERE DATAPAGAMENTO is not NULL;
UPDATE RiconciliatiPoste_novembre
SET DATA_PAGAMENTO = '2' || SUBSTRING(DATAPAGAMENTO,7,10) || SUBSTRING(DATAPAGAMENTO,0,3) || '0' || SUBSTRING(DATAPAGAMENTO,4,1)
WHERE DATAPAGAMENTO in ('11/1/2022','11/2/2022', '11/3/2022','11/4/2022','11/5/2022','11/6/2022','11/7/2022','11/8/2022','11/9/2022');

-- Accorpamento dei dati di novembre per i pagamenti PagoPa
CREATE TABLE PagoPa AS
SELECT *
FROM RiconciliatiPoste
union select *
from RiconciliatiPoste_novembre;

-- TABELLA: RiconciliatiPoste_dicembre
-- 1. Rimozione colonne superflue con informazioni ridondanti o non utili all'analisi
alter table RiconciliatiPoste_dicembre
drop TIPOIDCANALEPAGAMENTO;alter table RiconciliatiPoste_dicembre
drop DATADIACCREDITO;alter table RiconciliatiPoste_dicembre
drop TIPORICONCILIAZIONE;alter table RiconciliatiPoste_dicembre
drop DATARICONCILIAZIONE;alter table RiconciliatiPoste_dicembre
drop RIFERIMENTIDIRICONCILIAZIONE;alter table RiconciliatiPoste_dicembre
drop "IDENTIFICATIVO FLUSSO NODO         ";alter table RiconciliatiPoste_dicembre
drop IBANACCREDITO;alter table RiconciliatiPoste_dicembre
drop DOMINIOTITOLARE;alter table RiconciliatiPoste_dicembre
drop INDICEQUOTA;alter table RiconciliatiPoste_dicembre
drop field42;
-- 2. Rinominate le colonne in modo da essere congruenti con la tabella RiconciliatiPoste
ALTER TABLE RiconciliatiPoste_dicembre
RENAME COLUMN "TIPO RIF. CREDITORE" TO "TIPORIF.CREDITORE";ALTER TABLE RiconciliatiPoste_dicembre
RENAME COLUMN "TIPO AVVISO" TO TIPOAVVISO;ALTER TABLE RiconciliatiPoste_dicembre
RENAME COLUMN "DOMINIO BENEFICIARIO" TO DOMINIOBENEFICIARIO;ALTER TABLE RiconciliatiPoste_dicembre
RENAME COLUMN "CODICERIF. CREDITORE" TO "CODICERIF.CREDITORE";
-- 3. Aggiunte nuove colonne
alter table RiconciliatiPoste_dicembre
add DATA_OPERAZIONE;
alter table RiconciliatiPoste_dicembre
add DATA_PAGAMENTO;
UPDATE RiconciliatiPoste_dicembre
SET DATA_OPERAZIONE = '20' || SUBSTRING(DATAOPERAZIONE,7,9) || SUBSTRING(DATAOPERAZIONE,4,2) || SUBSTRING(DATAOPERAZIONE,0,3)
WHERE DATAOPERAZIONE is not NULL;
UPDATE RiconciliatiPoste_dicembre
SET DATA_PAGAMENTO = '20' || SUBSTRING(DATAPAGAMENTO,7,9) || SUBSTRING(DATAPAGAMENTO,4,2) || SUBSTRING(DATAPAGAMENTO,0,3)
WHERE DATAPAGAMENTO is not NULL;
-- 4. Pulizia dei dati
update  RiconciliatiPoste_dicembre
set CONVENZIONATORE = SUBSTRING(CONVENZIONATORE,3,5),
    "COD.FISCALEENTE/PA" = SUBSTRING("COD.FISCALEENTE/PA",3,11),
    TIPOAVVISO = SUBSTRING("TIPOAVVISO",3,6),
    reteincasso = SUBSTRING("reteincasso",3,3),
    TIPOPAGAMENTOESEGUITO = SUBSTRING("TIPOPAGAMENTOESEGUITO",3,2),
    DOMINIOBENEFICIARIO = SUBSTRING("DOMINIOBENEFICIARIO",3,11),
    QUOTASECONDARIA = SUBSTRING("QUOTASECONDARIA",3,1),
    IMPORTOPAGATO = REPLACE(IMPORTOPAGATO,',','.'),
    IMPORTOAVVISO = REPLACE(IMPORTOAVVISO,',','.'),
    CAUSALE = REPLACE(CAUSALE,'="',''),
    anagraficaversante = REPLACE(anagraficaversante,'="',''),
    "CODICEAVVISO" = SUBSTRING("CODICEAVVISO",3,18),
    "DATIPA" =  REPLACE(DATIPA,'="',''),
    "IDVERSANTE" =  REPLACE(IDVERSANTE,'="','');
update RiconciliatiPoste_dicembre
set "CODICERIF.CREDITORE" =  replace("CODICERIF.CREDITORE", ' "', '');
update RiconciliatiPoste_dicembre
set "CODICERIF.CREDITORE" =  replace("CODICERIF.CREDITORE", '="', '');
update RiconciliatiPoste_dicembre
set TIPORATA =  replace(TIPORATA, '="', '');
update RiconciliatiPoste_dicembre
set TIPORATA =  replace(TIPORATA, '"', '');
update RiconciliatiPoste_dicembre
set CODICERATA =  replace(CODICERATA, '="', '');
update RiconciliatiPoste_dicembre
set CODICERATA =  replace(CODICERATA, '"', '');
update RiconciliatiPoste_dicembre
set IDVERSANTE =  replace(IDVERSANTE, ' ', '');
update RiconciliatiPoste_dicembre
set IDVERSANTE =  replace(IDVERSANTE, '"', '');

-- Accorpamento dei dati di dicembre per i pagamenti PagoPa
CREATE TABLE RiconciliatiPoste_totale AS
SELECT *
FROM PagoPa
union select *
from RiconciliatiPoste_dicembre;

-- 1. Pulizia finale dei dati
update RiconciliatiPoste_totale
set CONTODIACCREDITO =  replace(CONTODIACCREDITO, '="', '');
update RiconciliatiPoste_totale
set CONTODIACCREDITO =  replace(CONTODIACCREDITO, ' "', '');
update RiconciliatiPoste_totale
set NOMEDELSERVIZIO =  replace(NOMEDELSERVIZIO, '="', '');
update RiconciliatiPoste_totale
set NOMEDELSERVIZIO =  replace(NOMEDELSERVIZIO, ' "', '');
update RiconciliatiPoste_totale
set TIPODATIPA =  replace(TIPODATIPA, '=', '');
update RiconciliatiPoste_totale
set TIPODATIPA =  replace(TIPODATIPA, ' "', '');
update RiconciliatiPoste_totale
set TIPODATIPA =  replace(TIPODATIPA, '"', '');
update RiconciliatiPoste_totale
set DATIPA =  replace(DATIPA, '=', '');
update RiconciliatiPoste_totale
set DATIPA =  replace(DATIPA, ' "', '');
update RiconciliatiPoste_totale
set IDENTIFICATIVOPSP =  replace(IDENTIFICATIVOPSP, '=', '');
update RiconciliatiPoste_totale
set IDENTIFICATIVOPSP =  replace(IDENTIFICATIVOPSP, ' "', '');
update RiconciliatiPoste_totale
set IDENTIFICATIVOPSP =  replace(IDENTIFICATIVOPSP, '"', '');
update RiconciliatiPoste_totale
set TIPOPAGAMENTOESEGUITO =  replace(TIPOPAGAMENTOESEGUITO, '=', '');
update RiconciliatiPoste_totale
set DOMINIOBENEFICIARIO =  replace(DOMINIOBENEFICIARIO, '=', '');
update RiconciliatiPoste_totale
set DOMINIOBENEFICIARIO =  replace(DOMINIOBENEFICIARIO, ' ', '');
update RiconciliatiPoste_totale
set QUOTASECONDARIA =  replace(QUOTASECONDARIA, '=', '');
update RiconciliatiPoste_totale
set CONTODIACCREDITO =  replace(CONTODIACCREDITO, '=', '');
update RiconciliatiPoste_totale
set NOMEDELSERVIZIO =  replace(NOMEDELSERVIZIO, '=', '');
update RiconciliatiPoste_totale
set "TIPORIF.CREDITORE" =  replace( "TIPORIF.CREDITORE" , '=', '');
update RiconciliatiPoste_totale
set "TIPORIF.CREDITORE" =  replace( "TIPORIF.CREDITORE" , ' "', '');
update RiconciliatiPoste_totale
set "TIPORIF.CREDITORE" =  replace( "TIPORIF.CREDITORE" , '"', '');
update RiconciliatiPoste_totale
set CAUSALE=  replace( CAUSALE , ' "', '');
-- 2. Aggiunte nuove colonne
ALTER TABLE RiconciliatiPoste_totale
ADD DATA_SCADENZA;
UPDATE RiconciliatiPoste_totale
    SET DATA_SCADENZA = '20' || SUBSTRING(DATASCADENZA,7,4)  || SUBSTRING(DATASCADENZA,4,2) || SUBSTRING(DATASCADENZA,0,3)
WHERE RiconciliatiPoste_totale.DATASCADENZA is not NULL;
-- 3. Rimozione colonne superflue con informazioni ridondanti o non utili all'analisi
ALTER TABLE RiconciliatiPoste_totale
DROP DATASCADENZA;
alter table RiconciliatiPoste_totale
drop "ENTE/PA";alter table RiconciliatiPoste_totale
drop "COD.FISCALEENTE/PA";alter table RiconciliatiPoste_totale
drop SERVIZIO;alter table RiconciliatiPoste_totale
drop CODICEIDCANALEPAGAMENTO;alter table RiconciliatiPoste_totale
drop DENOMINAZIONEBENEFICIARIO;alter table RiconciliatiPoste_totale
drop DATAOPERAZIONE;alter table RiconciliatiPoste_totale
drop DATAPAGAMENTO;alter table RiconciliatiPoste_totale
drop NOMEDELSERVIZIO;alter table RiconciliatiPoste_totale
drop "NOMEENTE/PA";alter table RiconciliatiPoste_totale
drop OPERAZIONE;alter table RiconciliatiPoste_totale
drop "TIPORIF.CREDITORE";alter table RiconciliatiPoste_totale
drop TIPOAVVISO;alter table RiconciliatiPoste_totale
drop TIPOPAGAMENTOESEGUITO;

-- TABELLA: Tares_3944_3950_365E_368E_TEFA
alter table Tares_3944_3950_365E_368E_TEFA
drop TipRec;alter table Tares_3944_3950_365E_368E_TEFA
drop ProgrForn;alter table Tares_3944_3950_365E_368E_TEFA
drop ProgrRip;alter table Tares_3944_3950_365E_368E_TEFA
drop FlagErCodTrib;alter table Tares_3944_3950_365E_368E_TEFA
drop FlagErAnno;alter table Tares_3944_3950_365E_368E_TEFA
drop ImpACred;alter table Tares_3944_3950_365E_368E_TEFA
drop FlagErCodFis;alter table Tares_3944_3950_365E_368E_TEFA
drop FlagErDatiIci;alter table Tares_3944_3950_365E_368E_TEFA
drop CodEnCom;alter table Tares_3944_3950_365E_368E_TEFA
drop Nome;alter table Tares_3944_3950_365E_368E_TEFA
drop Sesso;alter table Tares_3944_3950_365E_368E_TEFA
drop DataNasc;alter table Tares_3944_3950_365E_368E_TEFA
drop SecCodFis;alter table Tares_3944_3950_365E_368E_TEFA
drop IdSecCodFis;alter table Tares_3944_3950_365E_368E_TEFA
drop Denom;alter table Tares_3944_3950_365E_368E_TEFA
drop ComStaEst;alter table Tares_3944_3950_365E_368E_TEFA
drop Prov;alter table Tares_3944_3950_365E_368E_TEFA
drop TipImp;alter table Tares_3944_3950_365E_368E_TEFA
drop DetrazAbitPric;

-- TABELLA: Copie_Conformi
alter table Copie_Conformi
add DATA_CREAZIONE integer;
alter table Copie_Conformi
add DATA_MODIFICA integer;
alter table Copie_Conformi
add ANNO_MESE;
alter table Copie_Conformi
add DATA_CHIUSURA integer;

update Copie_Conformi
set DATA_CREAZIONE = substring("Data Creazione",7,4) || substring("Data Creazione",4,2) || substring("Data Creazione",0,3)
where "Data Creazione" is not null;

update Copie_Conformi
set DATA_MODIFICA = substring("Data Modifica",7,4) || substring("Data Modifica",4,2) || substring("Data Modifica",0,3)
where "Data Modifica" is not null;

update Copie_Conformi
set ANNO_MESE = substring("Anno-mese",7,4) || substring("Anno-mese",0,3) || substring("Anno-mese",4,2)
where "Anno-mese" is not null;

update Copie_Conformi
set DATA_CHIUSURA = substring(Copie_Conformi_1."Data Chiusura ",7,4) ||  substring(Copie_Conformi_1."Data Chiusura ",4,2) || substring(Copie_Conformi_1."Data Chiusura ",0,3)
where "Data Chiusura " is not null;

alter table Copie_Conformi
drop "Anno-mese";
alter table Copie_Conformi
drop "Data Modifica";
alter table Copie_Conformi
drop "Data Creazione";
alter table Copie_Conformi
drop "Data Chiusura ";

/*
 Query successive alla pulizia e riformattazione dei dati
 */
-- TABELLA: AVVISI - CONTRATTI (DOM)
create table DOM as
select *
from DOM_Avvisi_Contratti_Attivi_20230131
left join DOM_Contratti_Attivi_20220929
on DOM_Contratti_Attivi_20220929.UTZ_CONTRATTO = DOM_Avvisi_Contratti_Attivi_20230131.UTZ_CONTRATTO and
   DOM_Avvisi_Contratti_Attivi_20230131.CNT_COD=DOM_Contratti_Attivi_20220929.CNT_COD;
-- TABELLA: AVVISI - CONTRATTI (NDOM)
create table NDOM as
select *
from NDOM_Avvisi_Contratti_Attivi_20230131
left join NdomConParticelle
on NdomConParticelle.UTZ_CONTRATTO = NDOM_Avvisi_Contratti_Attivi_20230131.UTZ_CONTRATTO and
   NDOM_Avvisi_Contratti_Attivi_20230131.CNT_COD=NdomConParticelle.CNT_COD;

-- TABELLA: PagamentiNodoFiltrati
create table PagamentiNodoFiltrati as
    select "CODICERIF.CREDITORE",
            IDVERSANTE,
           IMPORTOAVVISO,
           ANAGRAFICAVERSANTE,
           IMPORTOPAGATO,
          DATA_OPERAZIONE,
           DATA_PAGAMENTO,
           DATA_SCADENZA,
           TIPORATA,
           CODICERATA,
           IMPORTOTOTALEACCREDITATO,
          RETEINCASSO,
           QUOTASECONDARIA
from RiconciliatiPoste_totale
where RiconciliatiPoste_totale."CODICERIF.CREDITORE" LIKE '1122%' and
      RiconciliatiPoste_totale.QUOTASECONDARIA in ('','N');
