/*INIZIO MACRO*/

proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\ANALISI_CRONICI_NUOVO.txt" new;
run;


%macro FREQ(reg=);


	*PRESTAZIONI FARMA- DRG;
		PROC SQL;
		CREATE TABLE temp.FARMA_01_&reg as select anno, cod_ricetta, data_erogazione, num_farma_ric ,cod_aic,
		quantita, costo_servizio, costo_acquisto, id_anonimo
		from output.FARMA_&reg
		WHERE anno=2019
		;QUIT;


		
		/*DA FARE ATTENZIONE!*/

		data temp.FARMA_01_&reg._a temp.FARMA_01_&reg._b;
		set temp.FARMA_01_&reg;
		if 0<=_n_<=20000000 then output temp.FARMA_01_&reg._a;
		if _n_  >20000000 then output temp.FARMA_01_&reg._b;
		run;


		proc sort  data=temp.FARMA_01_&reg._a;by cod_aic;run;
		proc sort  data=temp.FARMA_01_&reg._b;by cod_aic;run;

		data temp.farma_&reg._sort;
		merge temp.FARMA_01_&reg._a  temp.FARMA_01_&reg._b;
		by cod_aic;
		run;

		data temp.ddd_last_sort;
		set input.ddd_last;
		keep cod_aic atc;
		where atc like "A10A%" or atc like "A10B%";
		rename atc=id_atc;
		run;

		data temp.far_&reg._atc;
		merge temp.farma_&reg._sort (in = a) temp.ddd_last_sort (in = b);
		by cod_aic;
		if a;
		run;

		/**/
		PROC SQL;
		CREATE TABLE temp.FARMA_DIAB_&reg as select distinct ID_ANONIMO, "&&reg." as REGIONE,
		id_atc 
		from temp.far_&reg._atc
		where id_atc <> ""
		GROUP BY id_atc
		;QUIT;

		PROC SORT DATA=temp.FARMA_DIAB_&reg NODUPKEY;BY id_anonimo;RUN;

		proc delete data=temp.FARMA_01_&reg temp.far_&reg._atc temp.farma_&reg._sort
		;run;

%mend;




%FREQ(reg=010);
%FREQ(reg=020);
%FREQ(reg=030);
%FREQ(reg=041);
%FREQ(reg=042);
%FREQ(reg=050);
%FREQ(reg=060);
%FREQ(reg=070);
%FREQ(reg=080);
%FREQ(reg=090);
%FREQ(reg=100);
%FREQ(reg=110);
%FREQ(reg=120);
%FREQ(reg=130);
%FREQ(reg=140);
%FREQ(reg=150);
%FREQ(reg=160);
%FREQ(reg=170);
%FREQ(reg=180);
%FREQ(reg=190);
%FREQ(reg=200);


data temp.FARMA_DIAB;
set temp.FARMA_DIAB_010 temp.FARMA_DIAB_020 temp.FARMA_DIAB_030 temp.FARMA_DIAB_041
temp.FARMA_DIAB_042 temp.FARMA_DIAB_050 temp.FARMA_DIAB_060 temp.FARMA_DIAB_070
temp.FARMA_DIAB_080 temp.FARMA_DIAB_090 temp.FARMA_DIAB_100 temp.FARMA_DIAB_110
temp.FARMA_DIAB_120 temp.FARMA_DIAB_130 temp.FARMA_DIAB_140 temp.FARMA_DIAB_150
temp.FARMA_DIAB_160 temp.FARMA_DIAB_170 temp.FARMA_DIAB_180 temp.FARMA_DIAB_190
temp.FARMA_DIAB_200;
run;

proc sort data=temp.FARMA_DIAB/*285.912*/ NODUPKEY out=FARMA_DIAB /*285.912*/;BY id_anonimo;RUN;


PROC SQL;
select COUNT(distinct ID_anonimo) as persone
from FARMA_DIAB
;QUIT;


PROC SQL;
select COUNT(distinct ID_anonimo) as persone
from FARMA_DIAB
;QUIT;

/*tabella L4*/
data l4;
set 
temp.finale_l4_010 temp.finale_l4_020 temp.finale_l4_030 temp.finale_l4_041
temp.finale_l4_042 temp.finale_l4_050 temp.finale_l4_060 temp.finale_l4_070
temp.finale_l4_080 temp.finale_l4_090 temp.finale_l4_100 temp.finale_l4_110
temp.finale_l4_120 temp.finale_l4_130 temp.finale_l4_140 temp.finale_l4_150
temp.finale_l4_160 temp.finale_l4_170 temp.finale_l4_180 temp.finale_l4_190
temp.finale_l4_200;
run;


data l3;
set 
temp.finale_l3_010 temp.finale_l3_020 temp.finale_l3_030 temp.finale_l3_041
temp.finale_l3_042 temp.finale_l3_050 temp.finale_l3_060 temp.finale_l3_070
temp.finale_l3_080 temp.finale_l3_090 temp.finale_l3_100 temp.finale_l3_110
temp.finale_l3_120 temp.finale_l3_130 temp.finale_l3_140 temp.finale_l3_150
temp.finale_l3_160 temp.finale_l3_170 temp.finale_l3_180 temp.finale_l3_190
temp.finale_l3_200;
run;



data l2;
set 
 temp.l2_wrk_010  temp.l2_wrk_020  temp.l2_wrk_030  temp.l2_wrk_041
 temp.l2_wrk_042  temp.l2_wrk_050  temp.l2_wrk_060  temp.l2_wrk_070
 temp.l2_wrk_080  temp.l2_wrk_090  temp.l2_wrk_100  temp.l2_wrk_110
 temp.l2_wrk_120  temp.l2_wrk_130  temp.l2_wrk_140  temp.l2_wrk_150
 temp.l2_wrk_160  temp.l2_wrk_170  temp.l2_wrk_180  temp.l2_wrk_190
 temp.l2_wrk_200;
run;


%macro reg (reg=);
data finale_l1_&reg;
set 
temp.l1_sdo_wrk_&reg temp.l1_sism_diag_wrk_&reg temp.l1_hospice_wrk_&reg temp.l1_sdo_diag_wrk_&reg 
temp.l1_sdo_drg_wrk_&reg temp.l1_amb_wrk_&reg temp.l1_far_wrk_&reg temp.l1_ese_wrk_&reg temp.l1_sdo_interv_wrk_&reg
temp.l1_ps_ese_wrk_&reg temp.l1_ps_diag_wrk_&reg temp.l1_ps_amb_wrk_&reg
;run;
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




data l1;
set 
 finale_l1_010  finale_l1_020  finale_l1_030  finale_l1_041
 finale_l1_042  finale_l1_050  finale_l1_060  finale_l1_070
 finale_l1_080  finale_l1_090  finale_l1_100  finale_l1_110
 finale_l1_120  finale_l1_130  finale_l1_140  finale_l1_150
 finale_l1_160  finale_l1_170  finale_l1_180  finale_l1_190
 finale_l1_200;
keep id_anonimo id_indicatore;
run;


/*L4*/

proc sql;
create table l4_diabete as select distinct ID_ANONIMO ,ID_INDICATORE
from l4 /*temp.l2_wrk_041*/
where substr(ID_INDICATORE,1,4) in ("DI01","DI02","DI03","DI04")
;quit;

PROC SQL;/*176.928*/
select COUNT(distinct ID_anonimo) as persone
from l4_diabete
;QUIT;

/*l3*/
proc sql;
create table l3_diabete as select distinct ID_ANONIMO ,ID_INDICATORE
from l3 /*temp.l2_wrk_041*/
where substr(ID_INDICATORE,1,4) in ("DI01","DI02","DI03","DI04")
;quit;

PROC SQL;/*180.579*/
select COUNT(distinct ID_anonimo) as persone
from l3_diabete
;QUIT;


/*l2*/

proc sql;
create table l2_diabete as select distinct ID_ANONIMO ,ID_INDICATORE
from l2 /*temp.l2_wrk_041*/
where substr(ID_INDICATORE,1,4) in ("DI01","DI02","DI03","DI04")
;quit;

PROC SQL;/*180.579*/
select COUNT(distinct ID_anonimo) as persone
from l2_diabete
;QUIT;



/*L1*/

proc sql;
create table l1_diabete as select distinct ID_ANONIMO ,ID_INDICATORE
from l1 /*temp.l2_wrk_041*/
where substr(ID_INDICATORE,1,4) in ("DI01","DI02","DI03","DI04")
;quit;

proc sort data=l1_diabete out=senza_duplicati nodupkey;by id_anonimo;run;/*281.648*/

PROC SQL;/*281.648*/
select COUNT(distinct ID_anonimo) as persone
from l1_diabete
;QUIT;


/*-------------------------------------------------------------------*/
/*ulteriore analisi L1 PER GLI ATC per 041 -- > 0*/
proc sql;/*questo � un dato vecchio*/
create table L1_diabete_ATC as select distinct ID_ANONIMO ,ID_INDICATORE
from temp.l1_far_wrk_041 
where substr(ID_INDICATORE,1,4) in ("DI01","DI02","DI03","DI04")
;quit;


PROC SQL;
select COUNT(distinct ID_anonimo) as persone
from L1_diabete_ATC
;QUIT;

data L1_diabete_ATC1 ; 
set L1_diabete_ATC ;
where ID_INDICATORE contains ("_ATC");
run;


PROC SQL;/*2.315*/
select COUNT(distinct ID_anonimo) as persone
from L1_diabete_ATC1
;QUIT;

proc sql;/*questo � 041 NUOVO*/
create table L1_diabete_ATC as select distinct ID_ANONIMO ,ID_INDICATORE
from temp.l1_far_wrk_041 
where substr(ID_INDICATORE,1,4) in ("DI01","DI02","DI03","DI04")
;quit;


PROC SQL;
select COUNT(distinct ID_anonimo) as persone
from L1_diabete_ATC
;QUIT;

data L1_diabete_ATC1 ; 
set L1_diabete_ATC ;
where ID_INDICATORE contains ("_ATC");
run;


PROC SQL;/*2.315*/
select COUNT(distinct ID_anonimo) as persone
from L1_diabete_ATC1
;QUIT;








/*
data temp.FARMA_&PAT;
set temp.FARMA_&PAT._010 temp.FARMA_&PAT._020 temp.FARMA_&PAT._030 temp.FARMA_&PAT._041
temp.FARMA_&PAT._042 temp.FARMA_&PAT._050 temp.FARMA_&PAT._060 temp.FARMA_&PAT._070
temp.FARMA_&PAT._080 temp.FARMA_&PAT._090 temp.FARMA_&PAT._100 temp.FARMA_&PAT._110
temp.FARMA_&PAT._120 temp.FARMA_&PAT._130 temp.FARMA_&PAT._140 temp.FARMA_&PAT._150
temp.FARMA_&PAT._160 temp.FARMA_&PAT._170 temp.FARMA_&PAT._180 temp.FARMA_&PAT._190
temp.FARMA_&PAT._200;
run;

PROC SQL;
CREATE TABLE temp.FARMA_&PAT as select distinct ID_PATOLOGIA,ID_ATC,sum(freq) as freq , 
sum(distinct persone) as persone,regione
from temp.FARMA_&PAT
GROUP BY ID_ATC,regione
;QUIT;


proc export data = temp.AREA_FARMA
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\FARMA\FREQ_FARMA.xlsx"
	dbms = xlsx
	replace;
run;
*/
