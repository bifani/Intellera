OPTIONS MPRINT;
OPTIONS COMPRESS =YES;
proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\DATI_INPUT.txt" new;
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

/*Per ottimizzare tutto l'algoritmo del codice SAS si ha deciso di dividere tutto gli scripts in Regione di Residenza */

/*SDO*/
%macro sdo(reg=);
data ANAGRAFICA_CAMPIONE&&reg;
set camp.anagrafica_4mln (rename=( cod_regione=reg));
keep cuni id_anonimo reg;
where reg="&&reg.";
run;

proc sql;
create table output.SDO_&reg as select a.*,b.reg,b.id_anonimo
from INPUT.SDO as a join ANAGRAFICA_CAMPIONE&&reg as b
on a.cuni=b.cuni
where a.anno <> 2020
;quit;

proc delete data= ANAGRAFICA_CAMPIONE&&reg ;run;

%mend;

/*FARMACEUTICA*/

%macro FARMA(reg=);
data ANAGRAFICA_CAMPIONE&&reg;
set camp.anagrafica_4mln (rename=( cod_regione=reg));
keep cuni id_anonimo reg;
where reg="&&reg.";
run;

%macro ciclo(n=,stop=);

%do i = &n %to &stop;
proc sql;
create table output.FARMA_&i._&reg as select a.*,b.reg,b.id_anonimo
from INPUT.FARMA_&i as a join ANAGRAFICA_CAMPIONE&&reg as b
on a.cuni=b.cuni
;quit;
%end;
%mend;

%ciclo(n=2017,stop=2019);

data output.FARMA_&reg;
set output.FARMA_2017_&reg output.FARMA_2018_&reg output.FARMA_2019_&reg;
run;

proc delete data=output.FARMA_2017_&reg output.FARMA_2018_&reg output.FARMA_2019_&reg;run;

%mend;


/*HOSPICE*/
%macro HOSPICE(reg=);
data ANAGRAFICA_CAMPIONE&&reg;
set camp.anagrafica_4mln (rename=( cod_regione=reg));
keep cuni id_anonimo reg;
where reg="&&reg.";
run;

proc sql;
create table output.HOSPICE_&reg as select a.*,b.reg,b.id_anonimo
from INPUT.HOSPICE as a join ANAGRAFICA_CAMPIONE&&reg as b
on a.cuni=b.cuni
where a.anno <> 2020
;quit;

data output.HOSPICE_&reg;
set output.HOSPICE_&reg;
VAL_HOSPICE=(DATA_DIMISS +1 - DATA_RIC)*tariffa_gg;
run;

%mend;


/*AMBULATORIALE*/

%macro AMB(reg=);
data ANAGRAFICA_CAMPIONE&&reg;
set camp.anagrafica_4mln (rename=( cod_regione=reg));
keep cuni id_anonimo reg;
where reg="&&reg.";
run;



%macro ciclo(n=,stop=);
%do i=&n %to &stop;

proc sql;
create table output.AMB_&i._&reg as select a.*,b.reg,b.id_anonimo
from INPUT.AMB_&i as a join ANAGRAFICA_CAMPIONE&&reg as b
on a.cuni=b.cuni
;quit;
%end;
%mend;
%ciclo(n=2017,stop=2019);

data output.AMB_&reg;
set output.AMB_2017_&reg(rename=(nm_quantita=mn_quantita)) output.AMB_2018_&reg output.AMB_2019_&reg;
run;

proc delete data=output.AMB_2017_&reg output.AMB_2018_&reg output.AMB_2019_&reg;run;

%mend;


/*PS*/
%macro PS(reg=);
data ANAGRAFICA_CAMPIONE&&reg;
set camp.anagrafica_4mln (rename=( cod_regione=reg));
keep cuni id_anonimo reg;
where reg="&&reg.";
run;

PROC SQL;
CREATE TABLE OUTPUT.PS_&reg AS SELECT a.anno, a.cuni, a.cod_regione, a.data_arrivo, a.data_ora_arrivo, a.data_presa_incarico, a.data_ora_presa_incarico, a.data_dimiss, a.data_ora_dimiss,
a.cod_ese_pagamento,a.importo_lordo, a.id_prest_prim, a.id_prest_obi_prim_erog, a.id_prest_obi_prim_uscita,a.id_diag_prim, b.reg, b.id_anonimo
FROM INPUT.PS AS a JOIN ANAGRAFICA_CAMPIONE&&reg AS b
ON a.cuni=b.cuni
where a.anno <> 2020
;QUIT;

PROC DELETE DATA=ANAGRAFICA_CAMPIONE&&reg;
RUN;
%mend;


/*SISM*/
%macro SISM(reg=);
data ANAGRAFICA_CAMPIONE&&reg;
set camp.anagrafica_4mln (rename=( cod_regione=reg));
keep cuni id_anonimo reg;
where reg="&&reg.";
run;

PROC SQL;
CREATE TABLE OUTPUT.SISM_&reg AS SELECT a.*, b.reg, b.id_anonimo
FROM INPUT.SISM AS a JOIN ANAGRAFICA_CAMPIONE&&reg AS b
ON a.cuni=b.cuni
where a.anno <> 2020 
;QUIT;

PROC DELETE DATA=ANAGRAFICA_CAMPIONE&&reg;
RUN;
%mend;


/*FAR_TERRITORIALE*/
%macro FAR_TERRITORIALE(reg=);
data ANAGRAFICA_CAMPIONE&&reg;
set camp.anagrafica_4mln (rename=( cod_regione=reg));
keep cuni id_anonimo reg;
where reg="&&reg.";
run;

PROC SQL;
CREATE TABLE OUTPUT.FAR_TERRITORIALE_&reg AS SELECT a.*, b.reg, b.id_anonimo
FROM INPUT.FAR_TERRITORIALE AS a JOIN ANAGRAFICA_CAMPIONE&&reg AS b
ON a.cuni=b.cuni
where a.anno <> 2020 
;QUIT;

data OUTPUT.FAR_TERRITORIALE_&reg;
set OUTPUT.FAR_TERRITORIALE_&reg;
VAL_FAR_TERR=sum(QUOTA_SSR,QUOTA_UTENTE);
run;

PROC DELETE DATA=ANAGRAFICA_CAMPIONE&&reg;
RUN;
%mend;


/*OUTPUT*/
%sdo(reg=010);
%sdo(reg=020);
%sdo(reg=030);
%sdo(reg=041);
%sdo(reg=042);
%sdo(reg=050);
%sdo(reg=060);
%sdo(reg=070);
%sdo(reg=080);
%sdo(reg=090);
%sdo(reg=100);
%sdo(reg=110);
%sdo(reg=120);
%sdo(reg=130);
%sdo(reg=140);
%sdo(reg=150);
%sdo(reg=160);
%sdo(reg=170);
%sdo(reg=180);
%sdo(reg=190);
%sdo(reg=200);


%FARMA(reg=010);
%FARMA(reg=020);
%FARMA(reg=030);
%FARMA(reg=041);
%FARMA(reg=042);
%FARMA(reg=050);
%FARMA(reg=060);
%FARMA(reg=070);
%FARMA(reg=080);
%FARMA(reg=090);
%FARMA(reg=100);
%FARMA(reg=110);
%FARMA(reg=120);
%FARMA(reg=130);
%FARMA(reg=140);
%FARMA(reg=150);
%FARMA(reg=160);
%FARMA(reg=170);
%FARMA(reg=180);
%FARMA(reg=190);
%FARMA(reg=200);


%AMB(reg=010);
%AMB(reg=020);
%AMB(reg=030);
%AMB(reg=041);
%AMB(reg=042);
%AMB(reg=050);
%AMB(reg=060);
%AMB(reg=070);
%AMB(reg=080);
%AMB(reg=090);
%AMB(reg=100);
%AMB(reg=110);
%AMB(reg=120);
%AMB(reg=130);
%AMB(reg=140);
%AMB(reg=150);
%AMB(reg=160);
%AMB(reg=170);
%AMB(reg=180);
%AMB(reg=190);
%AMB(reg=200);


%HOSPICE(reg=010);
%HOSPICE(reg=020);
%HOSPICE(reg=030);
%HOSPICE(reg=042);
%HOSPICE(reg=041);
%HOSPICE(reg=050);
%HOSPICE(reg=060);
%HOSPICE(reg=070);
%HOSPICE(reg=080);
%HOSPICE(reg=090);
%HOSPICE(reg=100);
%HOSPICE(reg=110);
%HOSPICE(reg=120);
%HOSPICE(reg=130);
%HOSPICE(reg=140);
%HOSPICE(reg=150);
%HOSPICE(reg=160);
%HOSPICE(reg=170);
%HOSPICE(reg=180);
%HOSPICE(reg=190);
%HOSPICE(reg=200);

%PS(reg=010);
%PS(reg=020);
%PS(reg=030);
%PS(reg=041);
%PS(reg=042);
%PS(reg=050);
%PS(reg=060);
%PS(reg=070);
%PS(reg=080);
%PS(reg=090);
%PS(reg=100);
%PS(reg=110);
%PS(reg=120);
%PS(reg=130);
%PS(reg=140);
%PS(reg=150);
%PS(reg=160);
%PS(reg=170);
%PS(reg=180);
%PS(reg=190);
%PS(reg=200);

%SISM(reg=010);
%SISM(reg=020);
%SISM(reg=030);
%SISM(reg=041);
%SISM(reg=042);
%SISM(reg=050);
%SISM(reg=060);
%SISM(reg=070);
%SISM(reg=080);
%SISM(reg=090);
%SISM(reg=100);
%SISM(reg=110);
%SISM(reg=120);
%SISM(reg=130);
%SISM(reg=140);
%SISM(reg=150);
%SISM(reg=160);
%SISM(reg=170);
%SISM(reg=180);
%SISM(reg=190);
%SISM(reg=200);

%FAR_TERRITORIALE(reg=010);
%FAR_TERRITORIALE(reg=020);
%FAR_TERRITORIALE(reg=030);
%FAR_TERRITORIALE(reg=041);
%FAR_TERRITORIALE(reg=042);
%FAR_TERRITORIALE(reg=050);
%FAR_TERRITORIALE(reg=060);
%FAR_TERRITORIALE(reg=070);
%FAR_TERRITORIALE(reg=080);
%FAR_TERRITORIALE(reg=090);
%FAR_TERRITORIALE(reg=100);
%FAR_TERRITORIALE(reg=110);
%FAR_TERRITORIALE(reg=120);
%FAR_TERRITORIALE(reg=130);
%FAR_TERRITORIALE(reg=140);
%FAR_TERRITORIALE(reg=150);
%FAR_TERRITORIALE(reg=160);
%FAR_TERRITORIALE(reg=170);
%FAR_TERRITORIALE(reg=180);
%FAR_TERRITORIALE(reg=190);
%FAR_TERRITORIALE(reg=200);
