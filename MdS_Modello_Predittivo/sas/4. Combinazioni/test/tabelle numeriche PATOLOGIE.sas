proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\FREQ.txt" new;
run;

OPTIONS MPRINT;
OPTIONS COMPRESS =YES;

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

libname camp "T:\202204_modello\Output camp";


								/*CREO VARIABILE "FLAG_COMORBIDITA";*/

%macro reg(reg=);
data temp.AID_PAT_CRONICI_&reg;
set temp.ID_PAT_CRONICI_&reg;
REGIONE="&&reg.";
run;
%mend;
%reg(reg=010);
%reg(reg=020);
%reg(reg=030);
%reg(reg=041);
%reg(reg=042);
%reg(reg=050);
%reg(reg=060);
%reg(reg=070);
%reg(reg=080);
%reg(reg=090);
%reg(reg=100);
%reg(reg=110);
%reg(reg=120);
%reg(reg=130);
%reg(reg=140);
%reg(reg=150);
%reg(reg=160);
%reg(reg=170);
%reg(reg=180);
%reg(reg=190);
%reg(reg=200);


data temp.ID_PAT_CRONICI; 
set temp.AID_PAT_CRONICI_010 temp.AID_PAT_CRONICI_020 temp.AID_PAT_CRONICI_030 temp.AID_PAT_CRONICI_041 
temp.AID_PAT_CRONICI_042
temp.AID_PAT_CRONICI_050 temp.AID_PAT_CRONICI_060 temp.AID_PAT_CRONICI_070 temp.AID_PAT_CRONICI_080
temp.AID_PAT_CRONICI_090 temp.AID_PAT_CRONICI_100 temp.AID_PAT_CRONICI_110 temp.AID_PAT_CRONICI_120
temp.AID_PAT_CRONICI_130 temp.AID_PAT_CRONICI_140 temp.AID_PAT_CRONICI_150 temp.AID_PAT_CRONICI_160
temp.AID_PAT_CRONICI_170 temp.AID_PAT_CRONICI_180 temp.AID_PAT_CRONICI_190 temp.AID_PAT_CRONICI_200
;
run;

proc delete data=temp.AID_PAT_CRONICI_010 temp.AID_PAT_CRONICI_020 temp.AID_PAT_CRONICI_030 temp.AID_PAT_CRONICI_041 
temp.AID_PAT_CRONICI_042
temp.AID_PAT_CRONICI_050 temp.AID_PAT_CRONICI_060 temp.AID_PAT_CRONICI_070 temp.AID_PAT_CRONICI_080
temp.AID_PAT_CRONICI_090 temp.AID_PAT_CRONICI_100 temp.AID_PAT_CRONICI_110 temp.AID_PAT_CRONICI_120
temp.AID_PAT_CRONICI_130 temp.AID_PAT_CRONICI_140 temp.AID_PAT_CRONICI_150 temp.AID_PAT_CRONICI_160
temp.AID_PAT_CRONICI_170 temp.AID_PAT_CRONICI_180 temp.AID_PAT_CRONICI_190 temp.AID_PAT_CRONICI_200
;
run;

PROC SQL;
create table temp.ID_PAT_CRONICI2 as select
A.*,B.ETA,B.SESSO
from temp.ID_PAT_CRONICI AS  A LEFT JOIN camp.anagrafica_4mln AS  b
on a.id_anonimo = b.id_anonimo AND A.REGIONE=B.cod_regione
;QUIT;


PROC SORT DATA=temp.ID_PAT_CRONICI2;BY ID_ANONIMO ID_PATOLOGIA;WHERE ID_PATOLOGIA NE "";RUN;

DATA temp.ID_PAT_CRONICI2;
SET temp.ID_PAT_CRONICI2;
BY ID_ANONIMO;
N_PAT+1;
IF FIRST.ID_ANONIMO THEN N_PAT=1;
RUN;

proc sql;
create table id_anonimo as select distinct id_anonimo, count(n_pat) as sum
from temp.ID_PAT_CRONICI2
group by id_anonimo
;quit;

data temp.COMORBIDITA;
set id_anonimo;
if sum=1 then FLAG_COMORBIDITA=0;
if sum>1 then FLAG_COMORBIDITA=1;
run;

proc freq data=temp.COMORBIDITA;table flag_comorbidita;run;
 

/*Collego la tabella costo*/

/*creo macro-variabile*/

%macro ANALISI_CRONICI2(reg=);

*COSTO TOTALE;
proc sql;
create table temp.ACOSTO_TOT_&reg as select a.*,b.flag_comorbidita
from  temp.COSTO_TOT_&reg as a left join temp.COMORBIDITA as b
on a.id_anonimo=b.id_anonimo
;quit;


    *COSTO PER ETA CRONICI;

/*OUTPUT MONOPATOLOGICI*/

	data TEMP.ACOSTO_TOT_&reg._MONO;
	set TEMP.ACOSTO_TOT_&reg;
	where flag_comorbidita=0;
	run;
	
	data TEMP.ACOSTO_TOT_&reg._MONO;
	set TEMP.ACOSTO_TOT_&reg._MONO;
	length classe_eta $5.; 
	if eta < 15 then classe_eta = "0-14";
	else if eta >= 15 and eta < 45 then classe_eta = "15-44";
	else if eta >= 45 and eta < 65 then classe_eta = "45-64"; 
	else if eta >= 65 and eta < 75 then classe_eta = "65-74"; 
	else if eta >= 75 and eta < 85 then classe_eta = "75-84"; 
	else if eta >= 85 then classe_eta = "84 +"; 
	run;


	PROC SQL;
		CREATE TABLE TEMP.COSTOXETA_CRONICI_&reg._MONO AS
		SELECT distinct
			CLASSE_ETA,
			SESSO,
			COUNT(ID_ANONIMO) AS FREQ,
			MEAN(COALESCE(COSTO_AMB,0)) AS MEAN_VAL_AMB,
			MEAN(COALESCE(COSTO_SDO,0)) AS MEAN_VAL_RICOVERI,
			MEAN(COALESCE(COSTO_FARMA,0)) AS MEAN_VAL_FARMA,
			MEAN(COALESCE(COSTO_HOSPICE,0)) AS MEAN_VAL_HOSPICE,
			MEAN(COALESCE(COSTO_FARMA_TERR,0)) AS MEAN_VAL_FARMA_TERR,
			MEAN(COALESCE(COSTO_PS,0)) AS MEAN_VAL_PS,
			MEAN(VAL_TOT) AS MEAN_VAL, "&&reg." as REGIONE
		FROM TEMP.ACOSTO_TOT_&reg._MONO
		GROUP BY CLASSE_ETA, SESSO
		ORDER BY CLASSE_ETA, SESSO
	;QUIT;

		DATA TEMP.COSTOXETA_CRONICI_&reg._FILT_MONO;
	SET TEMP.COSTOXETA_CRONICI_&reg._MONO;
	WHERE FREQ GE 3;
	RUN;

	/*OUTPUT MULTIPATOLOGICI*/


	data TEMP.ACOSTO_TOT_&reg._MULTI;
	set TEMP.ACOSTO_TOT_&reg;
	where flag_comorbidita=1;
	run;

	data TEMP.ACOSTO_TOT_&reg._MULTI;
	set TEMP.ACOSTO_TOT_&reg;
	length classe_eta $5.; 
	if eta < 15 then classe_eta = "0-14";
	else if eta >= 15 and eta < 45 then classe_eta = "15-44";
	else if eta >= 45 and eta < 65 then classe_eta = "45-64"; 
	else if eta >= 65 and eta < 75 then classe_eta = "65-74"; 
	else if eta >= 75 and eta < 85 then classe_eta = "75-84"; 
	else if eta >= 85 then classe_eta = "84 +"; 
	run;


	PROC SQL;
		CREATE TABLE TEMP.COSTOXETA_CRONICI_&reg._MULTI AS
		SELECT distinct
			CLASSE_ETA,
			SESSO,
			COUNT(ID_ANONIMO) AS FREQ,
			MEAN(COALESCE(COSTO_AMB,0)) AS MEAN_VAL_AMB,
			MEAN(COALESCE(COSTO_SDO,0)) AS MEAN_VAL_RICOVERI,
			MEAN(COALESCE(COSTO_FARMA,0)) AS MEAN_VAL_FARMA,
			MEAN(COALESCE(COSTO_HOSPICE,0)) AS MEAN_VAL_HOSPICE,
			MEAN(COALESCE(COSTO_FARMA_TERR,0)) AS MEAN_VAL_FARMA_TERR,
			MEAN(COALESCE(COSTO_PS,0)) AS MEAN_VAL_PS,
			MEAN(VAL_TOT) AS MEAN_VAL, "&&reg." as REGIONE
		FROM TEMP.ACOSTO_TOT_&reg._MULTI
		GROUP BY CLASSE_ETA, SESSO
		ORDER BY CLASSE_ETA, SESSO
	;QUIT;

	DATA TEMP.COSTOXETA_CRONICI_&reg._FILT_MULTI;
	SET TEMP.COSTOXETA_CRONICI_&reg._MULTI;
	WHERE FREQ GE 3;
	RUN;

%mend;


%ANALISI_CRONICI2(reg=010);
%ANALISI_CRONICI2(reg=020);
%ANALISI_CRONICI2(reg=030);
%ANALISI_CRONICI2(reg=041);
%ANALISI_CRONICI2(reg=042);
%ANALISI_CRONICI2(reg=050);
%ANALISI_CRONICI2(reg=060);
%ANALISI_CRONICI2(reg=070);
%ANALISI_CRONICI2(reg=080);
%ANALISI_CRONICI2(reg=090);
%ANALISI_CRONICI2(reg=100);
%ANALISI_CRONICI2(reg=110);
%ANALISI_CRONICI2(reg=120);
%ANALISI_CRONICI2(reg=130);
%ANALISI_CRONICI2(reg=140);
%ANALISI_CRONICI2(reg=150);
%ANALISI_CRONICI2(reg=160);
%ANALISI_CRONICI2(reg=170);
%ANALISI_CRONICI2(reg=180);
%ANALISI_CRONICI2(reg=190);
%ANALISI_CRONICI2(reg=200);


%macro risultati(reg=); 

proc export data = TEMP.COSTOXETA_CRONICI_&reg._FILT_MONO
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\COSTOXETA_CRONICI_MONO_&reg._FILT.xlsx"
	dbms = xlsx
	replace;
run;

proc export data = TEMP.COSTOXETA_CRONICI_&reg._FILT_MULTI
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\COSTOXETA_CRONICI_MULTI_&reg._FILT.xlsx"
	dbms = xlsx
	replace;
run;
%mend;

%risultati(reg=010);
%risultati(reg=020);
%risultati(reg=030);
%risultati(reg=041);
%risultati(reg=042);
%risultati(reg=050);
%risultati(reg=060);
%risultati(reg=070);
%risultati(reg=080);
%risultati(reg=090);
%risultati(reg=100);
%risultati(reg=110);
%risultati(reg=120);
%risultati(reg=130);
%risultati(reg=140);
%risultati(reg=150);
%risultati(reg=160);
%risultati(reg=170);
%risultati(reg=180);
%risultati(reg=190);
%risultati(reg=200);


proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\FREQ2.txt" new;
run;

data pat; 
set TEMP.COSTOXETA_CRONICI_010_FILT_MONO TEMP.COSTOXETA_CRONICI_020_FILT_MONO TEMP.COSTOXETA_CRONICI_030_FILT_MONO 
TEMP.COSTOXETA_CRONICI_041_FILT_MONO TEMP.COSTOXETA_CRONICI_042_FILT_MONO TEMP.COSTOXETA_CRONICI_050_FILT_MONO
 TEMP.COSTOXETA_CRONICI_060_FILT_MONO TEMP.COSTOXETA_CRONICI_070_FILT_MONO TEMP.COSTOXETA_CRONICI_080_FILT_MONO
 TEMP.COSTOXETA_CRONICI_090_FILT_MONO TEMP.COSTOXETA_CRONICI_100_FILT_MONO TEMP.COSTOXETA_CRONICI_110_FILT_MONO
 TEMP.COSTOXETA_CRONICI_120_FILT_MONO TEMP.COSTOXETA_CRONICI_130_FILT_MONO TEMP.COSTOXETA_CRONICI_140_FILT_MONO
TEMP.COSTOXETA_CRONICI_150_FILT_MONO
  TEMP.COSTOXETA_CRONICI_160_FILT_MONO TEMP.COSTOXETA_CRONICI_170_FILT_MONO 
  TEMP.COSTOXETA_CRONICI_180_FILT_MONO TEMP.COSTOXETA_CRONICI_190_FILT_MONO TEMP.COSTOXETA_CRONICI_200_FILT_MONO;
run;


proc means data=pat sum;var freq;run;
