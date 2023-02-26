/* LIVELLO 4*/

OPTIONS MPRINT;
OPTIONS COMPRESS =YES;
proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\L4_FINALE.txt" new;
run;

/* libname dei dati iniziali di input, dati forniti dal ministero*/
libname  INPUT "T:\202204_modello\Input Stratificazione";
%let INPUT = INPUT;

/* La libname temporanea si chiamer� "temp", al momento impostata sulla work*/
libname  TEMP "T:\202204_modello\Output camp\temp";
%let TEMP = TEMP;

/* La Libname che conterr� le tabelle create la chiamo LIBRARY */
LIBNAME OUTPUT "T:\202204_modello\Output camp\output";
%let OUTPUT = OUTPUT;

libname camp "T:\202204_modello\Output camp";

%global check_indicatore;


%let dt_riferimento = "31122019";

%macro L4(reg=,ID_CLASSIFICAZIONE=);

							/*Diabete Mellito di tipo 1 complicato*/

/***********************************DI02*********************************************/
/*creo tabella L4 e tolgo le persone con diabete di tipo 1  "DI02", cio� la tabella senza la malattia da valutare*/


PROC SQL; 
CREATE TABLE temp.FINALE_L4_&reg AS
SELECT *
FROM temp.FINALE_L3_&reg 
WHERE ID_INDICATORE NOT IN ('DI02_AMB','DI02_DIA1','DI02_DRG','DI04_AMB','DI04_DIA1','DI04_DRG','NF02_INT1','NF02_AMB',
'ON01_SDO','ON01_AMB','ON02_SDO','ON02_AMB','ON03_ATC','ON05_ATC','ON05_AMB','ON06_ATC','ON06_AMB'
) and ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE
;QUIT;

%macro DI02(FLUSSO=);
/*tabella di persone con prestazione di diabete di tipo 1 nel flusso AMB/DRG/DIA1 "DI02"*/
data temp.tab_DI02_&FLUSSO._&reg;
set temp.FINALE_L3_&reg;
where ID_INDICATORE="DI02_&&FLUSSO." and ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE;
run;

/*creo un flag dove segnalo gli indicatori che abbiamo bisogno per il criterio*/
data temp.tab_DI02_&FLUSSO._&reg._crit;
set temp.FINALE_L3_&reg;
if ID_INDICATORE in ("DI01_DIA","DI01_DRG","DI01_ESE","DI01_ATC","DI01_AMB") then DI01=1;else DI01=0;
run;


/*faccio tabella per vedere i soggetti che rispettano il criterio richiesto, prendo solo i soggetti 
dello stesso ID_ANONIMO della tabella precedente "temp.tab_DI02_&FLUSSO_&reg"*/

proc sql;
create table temp.tab_DI02_&FLUSSO._&reg._crit1 as select ID_ANONIMO, ID_INDICATORE, ID_CLASSIFICAZIONE ,sum(DI01) as sum
from temp.tab_DI02_&FLUSSO._&reg._crit
where ID_ANONIMO in (select ID_ANONIMO from temp.tab_DI02_&FLUSSO._&reg) and ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE
group by ID_ANONIMO
having sum>0
;quit;

/*da ottimizzare*/


/*Dalla Tabella "FINALE_L3" prendo i sogetti "DI02_AMB" che hanno compiuto il criterio e poi
li "faccio passare" alla tabella FINALE_L4*/

proc sql;
create table temp.tab_DI02_&FLUSSO._&reg._crit2 as select ID_ANONIMO,ID_CLASSIFICAZIONE,ID_INDICATORE
from temp.FINALE_L3_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.tab_DI02_&FLUSSO._&reg._crit1) and ID_CLASSIFICAZIONE=9 and ID_INDICATORE="DI02_&&FLUSSO."
;quit;


PROC SQL;
				INSERT INTO temp.FINALE_L4_&reg (
						ID_CLASSIFICAZIONE
						, ID_INDICATORE
						, ID_ANONIMO
					)
					SELECT
						ID_CLASSIFICAZIONE,
						ID_INDICATORE,
						ID_ANONIMO
					FROM
						temp.tab_DI02_&FLUSSO._&reg._crit2
;quit;

proc delete data=temp.tab_DI02_&FLUSSO._&reg temp.tab_DI02_&FLUSSO._&reg._crit temp.tab_DI02_&FLUSSO._&reg._crit1
temp.tab_DI02_&FLUSSO._&reg._crit2
;run;

%mend;

%DI02(FLUSSO=AMB);
%DI02(FLUSSO=DRG);
%DI02(FLUSSO=DIA1);

										/*Diabete Mellito di tipo 2 complicato*/

/***********************************DI04*********************************************/
/*creo tabella L4 e tolgo le persone con diabete di tipo 2  "DI04", cio� la tabella senza la malattia da valutare*/

%macro DI04(FLUSSO=);
/*tabella di persone con prestazione di diabete di tipo 1 nel flusso AMB/DRG/DIA1 "DI04"*/
data temp.tab_DI04_&FLUSSO._&reg;
set temp.FINALE_L3_&reg;
where ID_INDICATORE="DI04_&&FLUSSO." and ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE;
run;

/*faccio tabella per vedere i soggetti che rispettano il criterio richiesto, prendo solo i soggetti 
dello stesso ID_ANONIMO della tabella precedente "temp.tab_DI04_AMB_&reg"*/
data temp.tab_DI04_&FLUSSO._&reg._crit;
set temp.FINALE_L3_&reg;
if ID_INDICATORE in ("DI03_DIA","DI03_DRG","DI03_ESE","DI03_ATC","DI03_AMB") then DI03=1;else DI03=0;
run;

proc sql;
create table temp.tab_DI04_&FLUSSO._&reg._crit1 as select ID_ANONIMO, ID_INDICATORE, ID_CLASSIFICAZIONE ,sum(DI03) as sum
from temp.tab_DI04_&FLUSSO._&reg._crit
where ID_ANONIMO in (select ID_ANONIMO from temp.tab_DI04_&FLUSSO._&reg) and ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE
group by ID_ANONIMO
having sum>0
;quit;


/*Dalla Tabella "FINALE_L3" prendo i sogetti "DI04_AMB" che hanno compiuto il criterio e poi
li "faccio passare" alla tabella FINALE_L4*/

proc sql;
create table temp.tab_DI04_&FLUSSO._&reg._crit2 as select ID_ANONIMO,ID_CLASSIFICAZIONE,ID_INDICATORE
from temp.FINALE_L3_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.tab_DI04_&FLUSSO._&reg._crit1) and ID_CLASSIFICAZIONE=9 and ID_INDICATORE="DI04_&&FLUSSO."
;quit;


PROC SQL;
				INSERT INTO temp.FINALE_L4_&reg (
						ID_CLASSIFICAZIONE
						, ID_INDICATORE
						, ID_ANONIMO
					)
					SELECT
						ID_CLASSIFICAZIONE,
						ID_INDICATORE,
						ID_ANONIMO
					FROM
						temp.tab_DI04_&FLUSSO._&reg._crit2
;quit;

proc delete data=temp.tab_DI04_&FLUSSO._&reg temp.tab_DI04_&FLUSSO._&reg._crit temp.tab_DI04_&FLUSSO._&reg._crit1
temp.tab_DI04_&FLUSSO._&reg._crit2
;run;

%mend;

%DI04(FLUSSO=AMB);
%DI04(FLUSSO=DRG);
%DI04(FLUSSO=DIA1);


					 /* ONCOLOGIA - Operato in trattamento adiuvante (con CT)*/
										/*VERSIONE 1*/

/***********************************ON01_SDO, ON01_AMB*********************************************/


/*tabella di persone Operato in trattamento adiuvante (con CT) :ON01_SDO, ON01_AMB*/
data temp.tab_ON01_SDO_&reg;
set temp.FINALE_L3_&reg;
where ID_INDICATORE IN ("ON01_SDO") and ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE;
run;

/*faccio tabella per vedere i soggetti che rispettano il criterio richiesto, prendo solo i soggetti 
dello stesso ID_ANONIMO della tabella precedente "temp.tab_ON01_AMB_&reg"*/
/*voglio prendere i soggetti che compiono il criterio*/
data temp.tab_ON01_SDO_&reg._crit;
set temp.FINALE_L3_&reg;
if ID_INDICATORE in ("ON01_AMB","0N01_DIA","ON01_DRG") then ON01=1;else ON01=0;
run;

proc sql;
create table temp.tab_ON01_SDO_&reg._crit1 as select ID_ANONIMO, ID_INDICATORE, ID_CLASSIFICAZIONE ,sum(ON01) as sum
from temp.tab_ON01_SDO_&reg._crit
where ID_ANONIMO in (select ID_ANONIMO from temp.tab_ON01_SDO_&reg) and ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE
group by ID_ANONIMO
having sum>0
;quit;

/*Dalla Tabella "FINALE_L3" prendo i sogetti "ON01_AMB" che hanno compiuto il criterio e poi
li "faccio passare" alla tabella FINALE_L4*/

proc sql;
create table temp.tab_ON01_SDO_&reg._crit2 as select ID_ANONIMO,ID_CLASSIFICAZIONE,ID_INDICATORE
from temp.FINALE_L3_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.tab_ON01_SDO_&reg._crit1) and ID_CLASSIFICAZIONE=9 and ID_INDICATORE="ON01_SDO"
;quit;


PROC SQL;
				INSERT INTO temp.FINALE_L4_&reg(
						ID_CLASSIFICAZIONE
						, ID_INDICATORE
						, ID_ANONIMO
					)
					SELECT
						ID_CLASSIFICAZIONE,
						ID_INDICATORE,
						ID_ANONIMO
					FROM
						temp.tab_ON01_SDO_&reg._crit2
;quit;

proc delete data=temp.tab_ON01_SDO_&reg temp.tab_ON01_SDO_&reg._crit temp.tab_ON01_SDO_&reg._crit1 temp.tab_ON01_SDO_&reg._crit2
;run;

						 /* ONCOLOGIA - Operato in trattamento adiuvante (solo RT)*/
											/*VERSIONE 1*/

/***********************************ON02_SDO, ON02_AMB*********************************************/


/*tabella di persone Operato in trattamento adiuvante (con CT) :ON02_SDO, ON02_AMB*/
data temp.tab_ON02_SDO_&reg;
set temp.FINALE_L3_&reg;
where ID_INDICATORE IN ("ON02_SDO") and ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE;
run;

/*faccio tabella per vedere i soggetti che rispettano il criterio richiesto, prendo solo i soggetti 
dello stesso ID_ANONIMO della tabella precedente "temp.tab_ON02_AMB_&reg"*/
data temp.tab_ON02_SDO_&reg._crit;
set temp.FINALE_L3_&reg;
if ID_INDICATORE in ("ON02_AMB","0N01_DIA","ON02_DRG") then ON02=1;else ON02=0;
run;

proc sql;
create table temp.tab_ON02_SDO_&reg._crit1 as select ID_ANONIMO, ID_INDICATORE, ID_CLASSIFICAZIONE ,sum(ON02) as sum
from temp.tab_ON02_SDO_&reg._crit
where ID_ANONIMO in (select ID_ANONIMO from temp.tab_ON02_SDO_&reg) and ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE 
group by ID_ANONIMO
having sum>0
;quit;

/*Dalla Tabella "FINALE_L3" prendo i sogetti "ON02_AMB" che hanno compiuto il criterio e poi
li "faccio passare" alla tabella FINALE_L4*/

proc sql;
create table temp.tab_ON02_SDO_&reg._crit2 as select ID_ANONIMO,ID_CLASSIFICAZIONE,ID_INDICATORE
from temp.FINALE_L3_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.tab_ON02_SDO_&reg._crit1) and ID_CLASSIFICAZIONE=9 and ID_INDICATORE="ON02_SDO"
;quit;


PROC SQL;
				INSERT INTO temp.FINALE_L4_&reg (
						ID_CLASSIFICAZIONE
						, ID_INDICATORE
						, ID_ANONIMO
					)
					SELECT
						ID_CLASSIFICAZIONE,
						ID_INDICATORE,
						ID_ANONIMO
					FROM
						temp.tab_ON02_SDO_&reg._crit2
;quit;

proc delete data=temp.tab_ON02_SDO_&reg temp.tab_ON02_SDO_&reg._crit temp.tab_ON02_SDO_&reg._crit1 temp.tab_ON02_SDO_&reg._crit2
;run;


			

/********************************** NF02_AMB , NF02_INT1 ********************************************/
/*Almeno 50 prestazioni annue tra ambulatoriale e procedure*/

/*ATTENZIONE , CI CONVIENE AGGIUNGERE LA VARIABILE ANNO NELLE TABELLE L1*/

/*faccio la somma delle prestazioni relative NF02_AMB quella del 2019, NF02_INT1*/


%let n=50; /*questo � il parametro che mi definisce ilnumero minimo di prestazione di dialisi da effettuare*/

/*persone che hanno almeno 50 prestazione di ambulatoriale o INT1 (interventi) annue*/


proc sql;
create table temp.NF02_AMB_&reg AS
SELECT
	&ID_CLASSIFICAZIONE as ID_CLASSIFICAZIONE,
	"NF02_AMB" as ID_INDICATORE,
	ID_ANONIMO,
	sum(NM_QUANTITA)  as quantita_amb/*da vedere bene nella lista*/
FROM output.amb_&reg
WHERE ID_ANONIMO in (select ID_anonimo from temp.finale_l3_&reg where ID_INDICATORE="NF02_AMB") 
and substr(ID_PRESTAZIONE,1,4) in ('3995','5498') and anno=2019
group by  ID_ANONIMO
;quit;


proc sql;
create table temp.NF02_INT1_&reg as select 
&ID_CLASSIFICAZIONE as ID_CLASSIFICAZIONE,
	"NF02_INT1" as ID_INDICATORE,
	ID_ANONIMO,
	count(*) as quantita_int/*da vedere bene nella lista*/
	FROM
	output.sdo_&reg
	WHERE ID_ANONIMO in (select ID_anonimo from temp.finale_l3_&reg where ID_INDICATORE="NF02_INT1") 
			and (
ID_INTERV_PRINC in ('3895','3995','5498') or 
ID_INTERV_SEC_1 in ('3895','3995','5498') or
ID_INTERV_SEC_2 in ('3895','3995','5498') or
ID_INTERV_SEC_3 in ('3895','3995','5498') or 
ID_INTERV_SEC_4 in ('3895','3995','5498') or 
ID_INTERV_SEC_5 in ('3895','3995','5498')
				) and anno=2019
group by ID_ANONIMO
;quit;


%let n=50; /*questo � il parametro che mi definisce ilnumero minimo di prestazione di dialisi da effettuare*/

/*persone che hanno almeno 50 prestazione di ambulatoriale o INT1 (interventi) annue*/

proc sql;
create table temp.NF02_&reg as select coalesce(a.ID_CLASSIFICAZIONE,b.ID_CLASSIFICAZIONE) as ID_CLASSIFICAZIONE,
coalesce(a.ID_INDICATORE,b.ID_INDICATORE) as ID_INDICATORE, coalesce(a.ID_ANONIMO,b.ID_ANONIMO) as ID_ANONIMO
from temp.NF02_AMB_&reg as a  full join temp.NF02_INT1_&reg as b 
on a.id_anonimo=b.id_anonimo 
where sum(coalesce(a.quantita_amb,0),coalesce(b.quantita_int,0)) >=&n
;quit;

proc sql;
create table temp.NF02_&reg._1 as select *
from temp.finale_l3_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.NF02_&reg) and ID_INDICATORE in ("NF02_AMB","NF02_INT1")
;quit;

proc sql;
INSERT INTO temp.FINALE_L4_&reg (
			ID_CLASSIFICAZIONE
			, ID_INDICATORE
			, ID_ANONIMO
		)
SELECT
	ID_CLASSIFICAZIONE,
	ID_INDICATORE,
	ID_ANONIMO
FROM
	temp.NF02_&reg._1
;quit;

proc delete data=/*temp.NF02_&reg*/ temp.NF02_INT1_&reg temp.NF02_AMB_&reg;run;



/*-----------------------------------------------------------------------------------------------------------------*/
												/*ON03_ATC*/

/*-----------------------------------------------------------------------------------------------------------------*/
							/*Oncologica - Operato in trattamento adiuvante (solo OT)*/
/*-----------------------------------------------------------------------------------------------------------------*/

/*� necessario che ci siano al massimo due farmaci tra quelli nella lista ON03_ATC*/


/*lista di ATC count(distinct atc) (tipologie diverse) join temp.l1_far_wrk_&reg- group by - ID_ANONIMO , ID_INDICATORE ,ID_CLASSIFICAZIONE*/
/**/
data temp.farma_&reg._a temp.farma_&reg._b;
		set output.farma_&reg(KEEP = anno cod_ricetta data_erogazione num_farma_ric cod_aic quantita costo_servizio costo_acquisto id_anonimo);
		if 0<=_n_<=20000000 then output temp.farma_&reg._a;
		if _n_  >20000000 then output temp.farma_&reg._b;
		run;


		proc sort  data=temp.farma_&reg._a;by cod_aic;run;
		proc sort  data=temp.farma_&reg._b;by cod_aic;run;

		data temp.farma_&reg._sort;
		merge temp.farma_&reg._a  temp.farma_&reg._b;
		by cod_aic;
		run;

		data temp.ddd_last_sort;
		set input.ddd_last;
		keep cod_aic atc;
		rename atc=id_atc;
		run;

		data temp.far_&reg._atc;
		merge temp.farma_&reg._sort (in = a) temp.ddd_last_sort (in = b);
		by cod_aic;
		if a;
		run;

	/*	proc sort data = input.ddd_last;
			by cod_aic;
		run; */

	/*filtro soltanto gli atc presenti dentro ai criteri traccianti*/
  
	proc sql;
		create table temp.atc_list as select 
			distinct id_atc 
			from input.crit_l1_far
			where id_classificazione = &id_classificazione;
	quit;

proc sql;
		create table temp.far_&reg._filt as select ID_ANONIMO, COUNT(distinct id_atc) as TIPI_ATC
		from temp.far_&reg._atc 
		where id_atc in (select id_atc from input.crit_l1_far where id_indicatore="ON03_ATC")  
		group by ID_ANONIMO,id_atc
		having TIPI_ATC<=2	
;quit;


proc sql;
create table temp.ON03_ATC_&reg as select ID_CLASSIFICAZIONE, ID_ANONIMO, ID_INDICATORE
from temp.finale_l3_&reg
where ID_INDICATORE="ON03_ATC" and ID_ANONIMO in (select ID_ANONIMO from temp.far_&reg._filt)
;quit;

/*
proc sql;
create table temp.ON03_ATC_&reg as select ID_CLASSIFICAZIONE, ID_ANONIMO, ID_INDICATORE
from temp.l1_far_wrk_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.finale_l3_&reg where ID_INDICATORE="ON03_ATC") and ID_INDICATORE = "ON03_ATC" 
and (NM_QUANT_FARMACO=. or NM_QUANT_FARMACO<=2)
;quit;*/

proc sql;
INSERT INTO temp.FINALE_L4_&reg (
			ID_CLASSIFICAZIONE
			, ID_INDICATORE
			, ID_ANONIMO
		)
SELECT
	ID_CLASSIFICAZIONE,
	ID_INDICATORE,
	ID_ANONIMO
FROM
	temp.ON03_ATC_&reg
;quit;


/*-----------------------------------------------------------------------------------------------------------------*/
												/*ON05_ATC*/
/*-----------------------------------------------------------------------------------------------------------------*/

/*-----------------------------------------------------------------------------------------------------------------*/
							/*Oncologica - Metastatico e Localmente avanzato*/
/*-----------------------------------------------------------------------------------------------------------------*/

/*� necessario che ci siano tre o pi� farmaci tra quelli nella lista ON05_ATC*/

/*
proc sql;
create table temp.ON05_ATC_&reg as select ID_CLASSIFICAZIONE, ID_ANONIMO, ID_INDICATORE
from temp.l1_far_wrk_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.finale_l3_&reg where ID_INDICATORE="ON05_ATC") and ID_INDICATORE = "ON05_ATC" 
and NM_QUANT_FARMACO>=3
;quit;
*/

proc sql;
		create table temp.far_&reg._filt as select *, COUNT(distinct id_atc) as TIPI_ATC
		from temp.far_&reg._atc /*tabella creata nello step precedente ON03_ATC*/
		where id_atc in (select id_atc from input.crit_l1_far where id_indicatore="ON05_ATC")  
		group by ID_ANONIMO,id_atc	
		having TIPI_ATC>=3
;quit;


proc sql;
create table temp.ON05_ATC_&reg as select ID_CLASSIFICAZIONE, ID_ANONIMO, ID_INDICATORE
from temp.finale_l3_&reg
where ID_INDICATORE="ON05_ATC" and ID_ANONIMO in (select ID_ANONIMO from temp.far_&reg._filt)
;quit;


/*aggiungo gli ID_INDICATORI che compiono i criteri*/
proc sql; 
INSERT INTO temp.FINALE_L4_&reg (
			ID_CLASSIFICAZIONE
			, ID_INDICATORE
			, ID_ANONIMO
		)
SELECT
	ID_CLASSIFICAZIONE,
	ID_INDICATORE,
	ID_ANONIMO
FROM
	temp.ON05_ATC_&reg
;quit;


/*--------------------------------------------ON05_ATC, ON05_AMB----------------------------------------------------------------*/

											/*Oncologia - Metastatico e Localmente avanzato*/
/*� necessario che ci siano almeno ON05_ATC, ON05_AMB per pi� di un anno*/


/*filtro nelle tabelle L1 gli indicatori di interese*/

data temp.l1_far_wrk_&reg._1;
set temp.l1_far_wrk_&reg;
where ID_INDICATORE="ON05_ATC";
run;

data temp.l1_amb_wrk_&reg._1;
set temp.l1_amb_wrk_&reg;
where ID_INDICATORE="ON05_AMB";
run;

%let mesi=6;

proc sql;
create table temp.ON05_&reg as select coalesce(a.ID_CLASSIFICAZIONE,b.ID_CLASSIFICAZIONE) as ID_CLASSIFICAZIONE,
coalesce(a.ID_INDICATORE,b.ID_INDICATORE) as ID_INDICATORE, coalesce(a.ID_ANONIMO,b.ID_ANONIMO) as ID_ANONIMO
from temp.l1_far_wrk_&reg._1 as a  full join temp.l1_amb_wrk_&reg._1 as b 
on a.id_anonimo=b.id_anonimo 
where  (max(a.DT_FIN_RIF,b.DT_FIN_RIF) +1 - min(a.DT_INI_RIF,b.DT_INI_RIF)) >=&mesi 
;quit;

proc sql;
create table temp.ON05_&reg._1 as select *
from temp.finale_l3_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.ON05_&reg) and ID_INDICATORE in ("ON05_AMB","ON05_ATC")
;quit;

/*
proc sql;
create table temp.ON05_&reg._1 as select ID_CLASSIFICAZIONE
			, ID_INDICATORE
			, ID_ANONIMO
from temp.finale_l3_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.ON05_&reg) and ID_INDICATORE="ON05_ATC"
;quit;
*/

proc sql; 
INSERT INTO temp.FINALE_L4_&reg (
			ID_CLASSIFICAZIONE
			, ID_INDICATORE
			, ID_ANONIMO
		)
SELECT
	ID_CLASSIFICAZIONE,
	ID_INDICATORE,
	ID_ANONIMO
FROM
	 temp.ON05_&reg._1 
;quit;



								/*Non operato con terapia primaria*/
/*****************************************ON06_ATC, ON06_AMB*********************************************************/


data temp.l1_far_wrk_&reg._1;
set temp.l1_far_wrk_&reg;
where ID_INDICATORE="ON06_ATC";
run;

data temp.l1_amb_wrk_&reg._1;
set temp.l1_amb_wrk_&reg;
where ID_INDICATORE="ON06_AMB";
run;

%let mesi=6;

proc sql;
create table temp.ON06_&reg as select  coalesce(a.ID_CLASSIFICAZIONE,b.ID_CLASSIFICAZIONE) as ID_CLASSIFICAZIONE,
coalesce(a.ID_INDICATORE,b.ID_INDICATORE) as ID_INDICATORE, coalesce(a.ID_ANONIMO,b.ID_ANONIMO) as ID_ANONIMO
from temp.l1_far_wrk_&reg as a  full join temp.l1_amb_wrk_&reg as b 
on a.id_anonimo=b.id_anonimo 
where  (max(a.DT_FIN_RIF,b.DT_FIN_RIF) +1 - min(a.DT_INI_RIF,b.DT_INI_RIF)) <&mesi 
;quit;

proc sql;
create table temp.ON06_&reg._1 as select *
from temp.finale_l3_&reg
where ID_ANONIMO in (select ID_ANONIMO from temp.ON06_&reg) and ID_INDICATORE in ("ON06_AMB","ON06_ATC")
;quit;


proc sql; 
INSERT INTO temp.FINALE_L4_&reg (
			ID_CLASSIFICAZIONE
			, ID_INDICATORE
			, ID_ANONIMO
		)
SELECT
	ID_CLASSIFICAZIONE,
	ID_INDICATORE,
	ID_ANONIMO
FROM
	temp.ON06_&reg._1
;quit;
%mend;


%L4(ID_CLASSIFICAZIONE=9,reg=010);
%L4(ID_CLASSIFICAZIONE=9,reg=020);
%L4(ID_CLASSIFICAZIONE=9,reg=030);
%L4(ID_CLASSIFICAZIONE=9,reg=041);
%L4(ID_CLASSIFICAZIONE=9,reg=042);
%L4(ID_CLASSIFICAZIONE=9,reg=050);
%L4(ID_CLASSIFICAZIONE=9,reg=060);
%L4(ID_CLASSIFICAZIONE=9,reg=070);
%L4(ID_CLASSIFICAZIONE=9,reg=080);
%L4(ID_CLASSIFICAZIONE=9,reg=090);
%L4(ID_CLASSIFICAZIONE=9,reg=100);
%L4(ID_CLASSIFICAZIONE=9,reg=110);
%L4(ID_CLASSIFICAZIONE=9,reg=120);
%L4(ID_CLASSIFICAZIONE=9,reg=130);
%L4(ID_CLASSIFICAZIONE=9,reg=140);
%L4(ID_CLASSIFICAZIONE=9,reg=150);
%L4(ID_CLASSIFICAZIONE=9,reg=160);
%L4(ID_CLASSIFICAZIONE=9,reg=170);
%L4(ID_CLASSIFICAZIONE=9,reg=180);
%L4(ID_CLASSIFICAZIONE=9,reg=190);
%L4(ID_CLASSIFICAZIONE=9,reg=200);
