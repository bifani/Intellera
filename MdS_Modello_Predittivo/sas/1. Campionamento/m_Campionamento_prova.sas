%macro lista_cuna();
/*lista cuna con regione 000*/
/*PROC SORT DATA=INPUT.AMB_2019 NODUPKEY OUT=;BY CUNI;RUN;

PROC SORT DATA=INPUT.SDO_2019;BY CUNI;RUN;

PROC SORT DATA=INPUT.FMC_2019;BY CUNI;RUN;*/

proc sql;
create table output.lista_cuna_000 as
select distinct cuni from INPUT.amb_2019 where cod_regione = "000"
union
select distinct cuni from INPUT.sdo_2019 where cod_regione = "000"
union
select distinct cuni from INPUT.fmc_2019 where cod_regione = "000";
quit;

proc sql;
create table output.lista_cuna_amb as
select distinct cuni from INPUT.amb_2019;
quit;

proc sql;
create table output.lista_cuna_sdo as
select distinct cuni from INPUT.sdo_2019;
quit;

proc sql;
create table output.lista_cuna_fmc as
select distinct cuni from INPUT.fmc_2019;
quit;

/*modifico
proc sql;
create table OUTPUT.lista_cuna_1 as
select cuni from output.lista_cuna_amb
union
select cuni from output.lista_cuna_sdo
union
select cuni from output.lista_cuna_fmc;
quit;*/
proc sql;
create table OUTPUT.lista_cuna_1 as
select cuni from output.lista_cuna_amb
union
select cuni from output.lista_cuna_sdo;
quit;



proc sql;
create table OUTPUT.lista_cuna_2 as
select cuni from OUTPUT.lista_cuna_2
union
select cuni from output.lista_cuna_fmc;
quit;


/* elimino i cuna aventi regione 000 */
proc sql;
create table OUTPUT.lista_cuna as select
* from OUTPUT.lista_cuna_2
where cuni not in (select cuni  from output.lista_cuna_000);
quit;
%mend;


/*Macro che conta e salva le righe del dataset fornito*/
%macro nrow_data(data_in=, macro_par=);
	%global &macro_par;
	proc sql noprint;
	select count(*)into:&macro_par from &data_in;
	quit;
%mend;

%macro n_camp(var=);
	/*Macro che calcola la dimensione del campione x variabili di proporzione*/
	%if &var='proporzione' %then %do;
		data _null_;
				q=quantile('NORMAL', &liv_conf);/*prima era .975*/
				call symputx('quantile', q);
		run;

		%let n_tot= %sysfunc(round(%sysevalf((&quantile**2)*(&p)*(1-&p)/(&e**2))));
		%if %sysevalf(&n_tot>&N_pop) %then %do;
		  	%let n_tot=&N_pop;
		%end;
	%end;
	/*Macro che calcola la dimensione del campione x costo medio*/
	%if &var='media' %then %do;
		data _null_;
				q=quantile('NORMAL', &liv_conf);/*prima era .975*/
				call symputx('quantile', q);
		run;

		%let n_tot= %sysfunc(round(%sysevalf((&quantile**2)*(&varianza))/(&e**2)));
		%if %sysevalf(&n_tot>&N_pop) %then %do;
		  	%let n_tot=&N_pop;
		%end;
	%end;
	%put La dimensione del campione �: &n_tot;
%mend;



/* campionamento_semplice */
%macro campionamento_semplice(dt_in=, dt_out=);
	proc surveyselect data=&dt_in  sampsize=&N_TOT out=&dt_out;
	run;

%mend;


/* campionamento_stratificato */
%macro campionamento_stratificato(data= , sesso=, eta=, regione=);
	proc sort data = &data;
	 BY &sesso &eta &regione;
	run;

	proc surveyselect data=&data n=&n_tot out=CAMPIONAMENTO_STRATIFICATO;
	   strata &sesso &eta &regione  / alloc=prop ;
	run;
%mend;
