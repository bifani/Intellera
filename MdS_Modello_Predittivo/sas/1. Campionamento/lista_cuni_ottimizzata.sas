   /*data _null_;
      rc=dlgcdir("T:\PROGRAMMI\Codice campionamento\Programmi staccati_20200503\WORK");
      put rc=;
   run;
   %put %sysfunc(pathname(work));*/



data OUTPUT.lista_cuna_2_1 output.lista_cuna_2_2 output.lista_cuna_2_3 output.lista_cuna_2_4;
set OUTPUT.lista_cuna_fmc;
if  _n_< 10000000 then output output.lista_cuna_2_1;
else if  10000000<=_n_ < 20000000 then output output.lista_cuna_2_2;
else if  20000000<=_n_ < 30000000 then output output.lista_cuna_2_3;
else if  _n_ >= 30000000 then output output.lista_cuna_2_4;
run;




proc sql;
create table OUTPUT.lista_cuna_2_5 as
select cuni from OUTPUT.lista_cuna_1
union
select cuni from output.lista_cuna_2_1;
quit;

proc sql;
create table OUTPUT.lista_cuna_2_6 as
select cuni from OUTPUT.lista_cuna_2_5
union
select cuni from output.lista_cuna_2_2;
quit;

proc sql;
create table OUTPUT.lista_cuna_2_7 as
select cuni from OUTPUT.lista_cuna_2_6
union
select cuni from output.lista_cuna_2_3;
quit;

proc sql;
create table OUTPUT.lista_cuna_2_8 as
select cuni from OUTPUT.lista_cuna_2_7
union
select cuni from output.lista_cuna_2_4;
quit;

/* elimino i cuna aventi regione 000 */
proc sql;
create table OUTPUT.lista_cuna as select
* from OUTPUT.lista_cuna_2_8
where cuni not in (select cuni  from output.lista_cuna_000);
quit;
