OPTIONS MPRINT;
OPTIONS COMPRESS =YES;
proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\l3_merge_3.txt" new;
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

libname camp "T:\202204_modello\Output camp";

%macro ANALISI_l3(ID_CLASSIFICAZIONE=,reg=);


proc sql;
	create table temp.merge_l3_&reg._1 as select a.*, b.sesso, b.eta from
		temp.finale_l3_&reg a left join
		(select id_anonimo, sesso, eta from camp.anagrafica_4mln where cod_regione = "&reg.") b
		on a.id_anonimo = b.id_anonimo;
quit;

data temp.merge_l3_&reg._2;
	set temp.merge_l3_&reg._1;
		if eta < 15 then classe_eta = 1;
		else if eta >= 15 and eta < 45 then classe_eta = 2;
		else if eta >= 45 and eta < 65 then classe_eta = 3;
		else if eta >= 65 and eta < 75 then classe_eta = 4;
		else if eta >= 75 and eta < 85 then classe_eta = 5;
		else if eta >= 85 then classe_eta = 6;
run;

proc sql;
	create table temp.merge_l3_&reg._etasesso as select
		id_classificazione,
		id_indicatore,
		sesso,
		classe_eta,
		count(distinct id_anonimo) as num_persone
	from temp.merge_l3_&reg._2
	group by id_classificazione,
		id_indicatore,
		sesso,
		classe_eta;
quit;

proc sql;
	create table temp.merge_l3_&reg as select
		id_classificazione,
		id_indicatore,
		count(distinct id_anonimo) as num_persone
	from temp.merge_l3_&reg._2
	group by id_classificazione,
		id_indicatore;
quit;


*aggancio la patologia;
proc sql;
	create table temp.merge_l3_&reg._4 as select
		a.*,
		b.id_patologia
	from temp.merge_l3_&reg._2 a
		inner join
		input.dominio_indicatori b
		on a.id_indicatore = b.id_indicatore;
quit;

proc sql;
	create table temp.l3_&reg._pat_etasesso as select
		id_classificazione,
		id_patologia,
		sesso,
		classe_eta,
		count(distinct id_anonimo) as num_persone
	from temp.merge_l3_&reg._4
	group by id_classificazione,
		id_patologia,
		sesso,
		classe_eta;
quit;

proc sql;
	create table temp.l3_&reg._pat as select
		id_classificazione,
		id_patologia,
		count(distinct id_anonimo) as num_persone
	from temp.merge_l3_&reg._4
	group by id_classificazione,
		id_patologia;
quit;

proc delete data = temp.merge_l3_&reg._2 temp.merge_l3_&reg._1;
run;

* preparo tabelle filtrate con num_persone >2;

data temp.merge_l3_&reg._etasesso_filt;
set temp.merge_l3_&reg._etasesso;
		where num_persone >2;
run;

data temp.merge_l3_&reg._filt;
set temp.merge_l3_&reg;
		where num_persone >2;
run;

data temp.l3_&reg._pat_etasesso_filt;
set temp.l3_&reg._pat_etasesso;
		where num_persone >2;
		REGIONE="&&reg.";
run;

data temp.l3_&reg._pat_filt;
set temp.l3_&reg._pat;
		where num_persone >2;
		REGIONE="&&reg.";
run;


/* ESPORTO I DATI AGGREGATI IN EXCEL */
/*
proc export data = temp.merge_l3_&reg._etasesso_filt
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\merge_l3_&reg._etasesso_filt.xlsx"
	dbms = xlsx
	replace;
run;

proc export data = temp.merge_l3_&reg._filt
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\merge_l3_&reg._filt.xlsx"
	dbms = xlsx
	replace;
run;


proc export data = temp.l3_&reg._pat_etasesso_filt
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\l3_&reg._pat_etasesso_filt.xlsx"
	dbms = xlsx
	replace;
run;


proc export data = temp.l3_&reg._pat_filt
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\l3_&reg._pat_fil.xlsx"
	dbms = xlsx
	replace;
run;
*/

%MEND;

%analisi_l3(ID_CLASSIFICAZIONE=9,reg=010);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=020);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=030);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=041);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=042);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=050);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=060);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=070);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=080);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=090);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=100);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=110);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=120);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=130);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=140);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=150);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=160);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=170);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=180);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=190);
%analisi_l3(ID_CLASSIFICAZIONE=9,reg=200);

data totale_l3;
set
temp.l3_010_pat_etasesso_filt temp.l3_020_pat_etasesso_filt temp.l3_030_pat_etasesso_filt temp.l3_041_pat_etasesso_filt
temp.l3_042_pat_etasesso_filt temp.l3_050_pat_etasesso_filt temp.l3_060_pat_etasesso_filt temp.l3_070_pat_etasesso_filt
temp.l3_080_pat_etasesso_filt temp.l3_090_pat_etasesso_filt temp.l3_100_pat_etasesso_filt temp.l3_110_pat_etasesso_filt
temp.l3_120_pat_etasesso_filt temp.l3_130_pat_etasesso_filt temp.l3_140_pat_etasesso_filt temp.l3_150_pat_etasesso_filt
temp.l3_160_pat_etasesso_filt temp.l3_170_pat_etasesso_filt temp.l3_180_pat_etasesso_filt temp.l3_190_pat_etasesso_filt
temp.l3_200_pat_etasesso_filt;
run;

proc export data = totale_l3
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\l3_pat_etasesso_filt.xlsx"
	dbms = xlsx
	replace;
run;
