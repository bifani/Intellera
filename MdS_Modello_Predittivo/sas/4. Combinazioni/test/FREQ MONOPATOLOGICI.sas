proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\ANALISI_CRONICI_TOTALE_FINALE.txt" new;
run;

OPTIONS MPRINT;
OPTIONS COMPRESS =YES;

/* libname dei dati iniziali di input, dati forniti dal ministero*/
libname  INPUT "T:\202204_modello\Input Stratificazione";

/* La libname temporanea si chiamer� "temp", al momento impostata sulla work*/
libname  TEMP "T:\202204_modello\Output camp\temp";

libname camp "T:\202204_modello\Output camp";

data temp.ID_PAT_CRONICI_TOT;*7.227.038;
set 
temp.ID_PAT_CRONICI_010 temp.ID_PAT_CRONICI_020 temp.ID_PAT_CRONICI_030 temp.ID_PAT_CRONICI_041
temp.ID_PAT_CRONICI_042 temp.ID_PAT_CRONICI_050 temp.ID_PAT_CRONICI_060 temp.ID_PAT_CRONICI_070
temp.ID_PAT_CRONICI_080 temp.ID_PAT_CRONICI_090 temp.ID_PAT_CRONICI_100 
temp.ID_PAT_CRONICI_110 temp.ID_PAT_CRONICI_120 temp.ID_PAT_CRONICI_130 temp.ID_PAT_CRONICI_140
temp.ID_PAT_CRONICI_150 temp.ID_PAT_CRONICI_160 temp.ID_PAT_CRONICI_170 temp.ID_PAT_CRONICI_180
temp.ID_PAT_CRONICI_190 temp.ID_PAT_CRONICI_200
;
run;
*7.157.131;
proc sort data=temp.ID_PAT_CRONICI_TOT;by ID_ANONIMO ID_PATOLOGIA;where ID_PATOLOGIA NE "";run;

data monopatologici;
set temp.ID_PAT_CRONICI_TOT;
by ID_ANONIMO;
n_pat+1;
if first.id_anonimo then n_pat=1;
run;

proc sql;
create table id_anonimo as select distinct id_anonimo , count(n_pat) as sum
from  monopatologici
group by id_anonimo
;quit;

data id_anonimo1;
set id_anonimo;
where sum=1;
run;

proc sql;
create table ID_PAT_CRONICI_TOT as select *
from  temp.ID_PAT_CRONICI_TOT
where id_anonimo in (select ID_ANONIMO from ID_ANONIMO1)
;quit;

proc sort data=ID_PAT_CRONICI_TOT;by ID_PATOLOGIA;run;

data ID_PAT_CRONICI_TOT1;
set ID_PAT_CRONICI_TOT;
length flussi $3.;
flussi=substr(ID_INDICATORE,6,8);
run;

data ID_PAT_CRONICI_TOT2;*650.294;
set ID_PAT_CRONICI_TOT1;
length flusso $15.;
if flussi in ("SDO","DRG","DIA","INT") then flusso="RICOVERI";
if flussi in ("ATC") then flusso="FARMACEUTICA";
if flussi in ("AMB") then flusso="AMBULATORIALE";
if flussi in ("ESE") then flusso="ESENZIONE";
drop flussi;
run;

	*elenco cronici;
	PROC SQL;
		create table ID_PAT_CRONICI_TOT3 as select
			A.*,
			B.ETA,
			B.SESSO
		from ID_PAT_CRONICI_TOT2 as A
		LEFT JOIN camp.anagrafica_4mln as B
		on a.id_anonimo = b.id_anonimo and a.regione=b.cod_regione
  ;QUIT;


proc freq data=ID_PAT_CRONICI_TOT3;table ID_PATOLOGIA*FLUSSO
/nopercent norow nocol out=freq_tot;
run;

proc freq data=ID_PAT_CRONICI_TOT3;table ID_PATOLOGIA*FLUSSO*sesso
/nopercent norow nocol out=freq_sex;
run;
