/*
data l4;
set
temp.finale_l4_010 temp.finale_l4_020 temp.finale_l4_030 temp.finale_l4_041 temp.finale_l4_042
temp.finale_l4_050 temp.finale_l4_060 temp.finale_l4_070 temp.finale_l4_080 temp.finale_l4_090
temp.finale_l4_100 temp.finale_l4_110 temp.finale_l4_120 temp.finale_l4_130 temp.finale_l4_140
temp.finale_l4_150 temp.finale_l4_160 temp.finale_l4_170 temp.finale_l4_180 temp.finale_l4_190
temp.finale_l4_200;
run;

data l3;
set
temp.finale_l3_010 temp.finale_l3_020 temp.finale_l3_030 temp.finale_l3_041 temp.finale_l3_042
temp.finale_l3_050 temp.finale_l3_060 temp.finale_l3_070 temp.finale_l3_080 temp.finale_l3_090
temp.finale_l3_100 temp.finale_l3_110 temp.finale_l3_120 temp.finale_l3_130 temp.finale_l3_140
temp.finale_l3_150 temp.finale_l3_160 temp.finale_l3_170 temp.finale_l3_180 temp.finale_l3_190
temp.finale_l3_200;
run;

data l2;
set
temp.l2_wrk_010 temp.l2_wrk_020 temp.l2_wrk_030 temp.l2_wrk_041 temp.l2_wrk_042
temp.l2_wrk_050 temp.l2_wrk_060 temp.l2_wrk_070 temp.l2_wrk_080 temp.l2_wrk_090
temp.l2_wrk_100 temp.l2_wrk_110 temp.l2_wrk_120 temp.l2_wrk_130 temp.l2_wrk_140
temp.l2_wrk_150 temp.l2_wrk_160 temp.l2_wrk_170 temp.l2_wrk_180 temp.l2_wrk_190
temp.l2_wrk_200;
run;

data l3_NU01_INT;
set l3;
where ID_INDICATORE = "NU01_INT";
run;

proc sql;* 45.303 ;
select count(distinct id_anonimo)
from l3_NU01_INT
;quit;

*/



										/*VERIFICA NU01 - EPILISSIA*/

/*controllo se c'� il criterio aggiunto rispetto al criterio scorso*/
data prova;
set input.crit_l1_sdo_proced;
where ID_INDICATORE in ("NU01_INT","NU01_INT2","NU01_INT3");
run;

/*tabella dei soggetti rintracciati in L1 SDO INTERVENTO*/

data soggetti_l1_NU01_INT;
set temp.l1_sdo_interv_wrk_030;
where ID_INDICATORE="NU01_INT";
run;

/*prendo solo le osservazioni che non hanno l'indicatore NU01_INT cio� l'epilessia nel flusso INTERVENTO SDO
dalla tabella dei criteri*/
data l4_NU01_INT;
set temp.finale_l4_030;
where ID_INDICATORE ne "NU01_INT";
run;

data l3_NU01_INT;
set temp.finale_l3_030;
where ID_INDICATORE ne "NU01_INT";
run;

data l2_NU01_INT;
set temp.l2_wrk_030;
where ID_INDICATORE ne "NU01_INT";
run;


/*CONTROLLO LA NUMEROSITA*/

/*verifico se ci sono persone diverse nella tabella totale di L2*/
proc sql;
create table l2_CONTROLLO as select *
from  l2_NU01_INT
where ID_ANONIMO in (select ID_ANONIMO from soggetti_l1_NU01_INT)
;quit;

/*6.913 */
proc sql;*6.913 soggetti trovati in lombardia dalla tabella totale l2 in cui hanno id_anonimo
uguale a quelli soggetti rintracciati nel livello L1 SDO_INTERVENTI;
select count(distinct id_anonimo)
from l2_CONTROLLO
;quit;

/*verifico se ci sono persone diverse nella tabella totale di L3*/
proc sql;
create table l3_CONTROLLO as select *
from  l3_NU01_INT
where ID_ANONIMO in (select ID_ANONIMO from soggetti_l1_NU01_INT)
;quit;

/*6.891*/
proc sql;*6.891 soggetti trovati in lombardia dalla tabella totale l3 in cui hanno id_anonimo
uguale a quelli soggetti rintracciati nel livello L1 SDO_INTERVENTI;
select count(distinct id_anonimo)
from l3_CONTROLLO
;quit;

/*verifico se ci sono persone diverse nella tabella totale di L4*/
proc sql;
create table l4_CONTROLLO as select *
from  l4_NU01_INT
where ID_ANONIMO in (select ID_ANONIMO from soggetti_l1_NU01_INT)
;quit;

/*6.891 */
proc sql;*6.891 soggetti trovati in lombardia dalla tabella totale l4 in cui hanno id_anonimo
uguale a quelli soggetti rintracciati nel livello L1 SDO_INTERVENTI;
select count(distinct id_anonimo)
from l4_CONTROLLO
;quit;


/*guardo quante persone sono stati rintracciati in L1 SDO INTERV*/
*6.996;
proc sql;*6.996 soggetti trovati in lombardia dalla tabella del livello L1 SDO_INTERVENTI;
select count(distinct id_anonimo)
from soggetti_l1_NU01_INT
;quit;

/*quali sono queste persone che non "superano" L2 , 83 soggetti*/


/*83*/
proc sql;
create table soggetti_not_L2 as
select *
from soggetti_l1_NU01_INT
where id_anonimo not in(select id_anonimo from l2_CONTROLLO)
;quit;

proc sql;
create table soggetti_in_L2 as
select *
from soggetti_l1_NU01_INT
where id_anonimo in(select id_anonimo from l2_CONTROLLO)
;quit;







/*cio� non le frequenze di NU01 non aumenta perch� abbiamo gi� indentificato questi soggeti
nei altri flussi*/


/*VERIFICA NU07 - Miopatia*/

data prova;
set input.crit_l1_amb;
where ID_INDICATORE="NU07_AMB";
run;

data l4;
set
temp.finale_l4_010 temp.finale_l4_020 temp.finale_l4_030 temp.finale_l4_041 temp.finale_l4_042
temp.finale_l4_050 temp.finale_l4_060 temp.finale_l4_070 temp.finale_l4_080 temp.finale_l4_090
temp.finale_l4_100 temp.finale_l4_110 temp.finale_l4_120 temp.finale_l4_130 temp.finale_l4_140
temp.finale_l4_150 temp.finale_l4_160 temp.finale_l4_170 temp.finale_l4_180 temp.finale_l4_190
temp.finale_l4_200;
run;

data l4_NU07_AMB;
set l4;
where ID_INDICATORE = "NU07_AMB";
run;


proc sql;*211.531;
select count(distinct id_anonimo)
from l4_NU07_AMB
;quit;


/*verifico se ci sono persone diverse nella tabella totale di L4*/
proc sql;
create table l4_CONTROLLO as select *
from  l4
where ID_ANONIMO in (select ID_ANONIMO from l4_NU07_AMB)
;quit;

proc sql;*211.531;
select count(distinct id_anonimo)
from l4_CONTROLLO
;quit;


/*VERIFICA NF01 - Malattia renale cronica dallo stadio 1 a stadio 5*/

data prova;
set input.crit_l1_amb;
where ID_INDICATORE="NF01_AMB";
run;

proc freq data=prova;table id_prestazione;run;

/*da verificare la numerosit�*/

/*PN01 - ASMA*/

data prova;
set input.crit_l1_amb;
where ID_INDICATORE in  ("PN01_AMB", "PN01_AMB1", "PN01_AMB2");
run;

proc freq data=prova;table id_prestazione;run;

data l3_PN01_AMB;
set l3;
where ID_INDICATORE in  ("PN01_AMB", "PN01_AMB1", "PN01_AMB2");
run;

data l4_PN01_AMB;
set l4;
where ID_INDICATORE in  ("PN01_AMB", "PN01_AMB1", "PN01_AMB2");
run;


data l2_PN01_AMB;
set l2;
where ID_INDICATORE in  ("PN01_AMB", "PN01_AMB1", "PN01_AMB2");
run;

proc sql;*64.435;
select count(distinct id_anonimo)
from l2_PN01_AMB
;quit;


/*verifico se ci sono persone diverse nella tabella totale di L4*/
proc sql;
create table l2_CONTROLLO as select *
from  l2
where ID_ANONIMO in (select ID_ANONIMO from l2_PN01_AMB)
;quit;

proc sql;*211.531;
select count(distinct id_anonimo)
from l2_CONTROLLO
;quit;
