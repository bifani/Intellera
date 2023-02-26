/* La Libname che conterr� le tabelle di input la chiamo LIBRARY */
LIBNAME INPUT "T:\202204_modello\Input camp";
LIBNAME OUTPUT "T:\202204_modello\Output camp";
%let p=0.5; /*per sicurezza fissiamo la deviazione standard al 50% */
%let e=0.001; /*fissiamo l'errore commesso al 2%*/
%let liv_conf=.995; /*definizione dell'a/2)*/
%let varianza=0.05; /*inserire la varianza della media totale del costo*/
%global p;
%global e;
%global liv_conf;
%global n_tot;

options compress=yes;
OPTIONS MPRINT;


data _null_;
      rc=dlgcdir("T:\PROGRAMMI\Codice campionamento\Programmi staccati_20200503\WORK");
      put rc=;
   run;

proc printto log='T:\PROGRAMMI\Codice campionamento\log_08062022_JOIN_2.txt' NEW;
run;



proc sql;
	create table output.sdo_wrk as 
	select *,
	case when sesso='1' then 'M'
		 when sesso ='2' then 'F'
		 else 'NA'
		 end as sesso_FM
	from input.sdo_2019;
	quit;

	data output.sdo_wrk (rename=(sesso_FM=sesso));
	set output.sdo_wrk (drop=sesso);
	run;

	data output.flussi_2019_wrk;
	set input.amb_2019(keep= cuni sesso eta cod_regione costo contatti_ssn) output.sdo_wrk(keep= cuni sesso eta cod_regione contatti_ssn)
	input.fmc_2019(keep= cuni sesso eta cod_regione costo contatti_ssn);
	run;

	/*proc delete data=output.sdo_wrk;
	run;*/

	/* tolgo la regione 000 */
	proc sql;
	create table output.flussi_2019_wrk as
	select *
	from output.flussi_2019_wrk
	where cuni not in (select cuni from output.lista_cuna_000);
	quit;

	data output.flussi_2019_wrk_1 output.flussi_2019_wrk_2 output.flussi_2019_wrk_3 output.flussi_2019_wrk_4
output.flussi_2019_wrk_5 output.flussi_2019_wrk_6 output.flussi_2019_wrk_7 output.flussi_2019_wrk_8;
set output.flussi_2019_wrk;
if  _n_< 20000000 then output output.flussi_2019_wrk_1;
else if  20000000 <=_n_ <  40000000 then output output.flussi_2019_wrk_2;
else if  40000000 <=_n_ <  60000000 then output output.flussi_2019_wrk_3;
else if  60000000 <=_n_ <  80000000 then output output.flussi_2019_wrk_4;
else if  80000000 <=_n_ <  100000000 then output output.flussi_2019_wrk_5;
else if  100000000 <=_n_ < 120000000 then output output.flussi_2019_wrk_6;
*else if  120000000 <=_n_ < 140000000 then output output.flussi_2019_wrk_7;
*else if  _n_ >= 140000000 then output output.flussi_2019_wrk_8;
run;

*data a;
*a=19999999+20000000+20000000+20000000+20000000+15165026;
*run;
*115.165.025 -115.165.025 ok;
	/* sesso */
	proc sql;
	create table output.flussi_sesso_1 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_2019_wrk_1
	group by cuni,sesso;
	quit;

	proc sql;
	create table output.flussi_sesso_2 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_2019_wrk_2
	group by cuni,sesso;
	quit;

	proc sql;
	create table output.flussi_sesso_3 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_2019_wrk_3
	group by cuni,sesso;
	quit;

	proc sql;
	create table output.flussi_sesso_4 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_2019_wrk_4
	group by cuni,sesso;
	quit;

	proc sql;
	create table output.flussi_sesso_5 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_2019_wrk_5
	group by cuni,sesso;
	quit;

	proc sql;
	create table output.flussi_sesso_6 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_2019_wrk_6
	group by cuni,sesso;
	quit;
	*13.840.704;

	/**********************************************************/

	/*unicamo le tabelle*/

proc printto log='T:\PROGRAMMI\Codice campionamento\log_08062022_sesso.txt' NEW;
run;

	data output.flussi_sesso_union1;
	set output.flussi_sesso_1 output.flussi_sesso_2;
	run;

	proc sql;
	create table output.flussi_sesso_union1 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_sesso_union1
	group by cuni,sesso;
	quit;

		data output.flussi_sesso_union2;
	set output.flussi_sesso_union1 output.flussi_sesso_3;
	run;

		proc sql;
	create table output.flussi_sesso_union2 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_sesso_union2
	group by cuni,sesso;
	quit;
	
	data output.flussi_sesso_union3;
	set output.flussi_sesso_union2 output.flussi_sesso_4;
	run;

	proc sql;
	create table output.flussi_sesso_union3 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_sesso_union3
	group by cuni,sesso;
	quit;

	data output.flussi_sesso_union4;
	set output.flussi_sesso_union3 output.flussi_sesso_5;
	run;

	proc sql;
	create table output.flussi_sesso_union4 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_sesso_union4
	group by cuni,sesso;
	quit;

	data output.flussi_sesso_union5;
	set output.flussi_sesso_union4 output.flussi_sesso_6;
	run;

	proc sql;
	create table output.flussi_sesso_union5 as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_sesso_union5
	group by cuni,sesso;
	quit;
*46.644.085;

	proc sort data=output.flussi_sesso_union5;
	by cuni contatti_ssn;
	run;

proc printto log='T:\PROGRAMMI\Codice campionamento\log_08062022_sesso.txt' NEW;
run;
	data output.flussi_sesso_fin;
	set output.flussi_sesso_union5;
	by cuni;
	retain last_unique;
	if last.cuni;
	run;

	/*girare da qui*/

	/* La Libname che conterr� le tabelle di input la chiamo LIBRARY */
LIBNAME INPUT "T:\202204_modello\Input camp";
LIBNAME OUTPUT "T:\202204_modello\Output camp";
%let p=0.5; /*per sicurezza fissiamo la deviazione standard al 50% */
%let e=0.001; /*fissiamo l'errore commesso al 2%*/
%let liv_conf=.995; /*definizione dell'a/2)*/
%let varianza=0.05; /*inserire la varianza della media totale del costo*/
%global p;
%global e;
%global liv_conf;
%global n_tot;

options compress=yes;
OPTIONS MPRINT;
%let n_tot=4000000;

proc printto log='T:\PROGRAMMI\Codice campionamento\log_08062022_reg.txt' NEW;
run;

/*reg*/

proc sql;
	create table output.flussi_reg_1 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_1
	group by cuni,cod_regione;
	quit;

proc sql;
	create table output.flussi_reg_2 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_2
	group by cuni,cod_regione;
	quit;

	proc sql;
	create table output.flussi_reg_3 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_3
	group by cuni,cod_regione;
	quit;

	proc sql;
	create table output.flussi_reg_4 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_4
	group by cuni,cod_regione;
	quit;

		proc sql;
	create table output.flussi_reg_5 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_5
	group by cuni,cod_regione;
	quit;

	proc sql;
	create table output.flussi_reg_6 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_6
	group by cuni,cod_regione;
	quit;
/*uniisci le tabelle*/
	data output.flussi_reg_union1;
	set output.flussi_reg_1 output.flussi_reg_2;
	run;

	proc sql;
	create table output.flussi_reg_union1 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from output.flussi_reg_union1
	group by cuni,cod_regione;
	quit;

	data output.flussi_reg_union2;
	set output.flussi_reg_union1 output.flussi_reg_3;
	run;

	proc sql;
	create table output.flussi_reg_union2 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from output.flussi_reg_union2
	group by cuni,cod_regione;
	quit;

	data output.flussi_reg_union3;
	set output.flussi_reg_union2 output.flussi_reg_4;
	run;

	proc sql;
	create table output.flussi_reg_union3 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from output.flussi_reg_union3
	group by cuni,cod_regione;
	quit;

	data output.flussi_reg_union4;
	set output.flussi_reg_union3 output.flussi_reg_5;
	run;

	proc sql;
	create table output.flussi_reg_union4 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from output.flussi_reg_union4
	group by cuni,cod_regione;
	quit;

	data output.flussi_reg_union5;
	set output.flussi_reg_union4 output.flussi_reg_6;
	run;

	proc sql;
	create table output.flussi_reg_union5 as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from output.flussi_reg_union5
	group by cuni,cod_regione;
	quit;

	proc sort data=output.flussi_reg_union5;
	by cuni contatti_ssn;
	run;

	data output.flussi_reg_fin;
	set output.flussi_reg_union5;
	by cuni;
	retain last_unique;
	if last.cuni;
	run;


	

	/*eta*/

proc sql;
	create table output.flussi_eta_1 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_1
	group by cuni,eta;
	quit;

proc sql;
	create table output.flussi_eta_2 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_2
	group by cuni,eta;
	quit;

	proc sql;
	create table output.flussi_eta_3 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_3
	group by cuni,eta;
	quit;

	proc sql;
	create table output.flussi_eta_4 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_4
	group by cuni,eta;
	quit;

		proc sql;
	create table output.flussi_eta_5 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_5
	group by cuni,eta;
	quit;

	proc sql;
	create table output.flussi_eta_6 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk_6
	group by cuni,eta;
	quit;
/*uniisci le tabelle*/
   data output.flussi_eta_union1;
   set output.flussi_eta_1 output.flussi_eta_2;
   run;

	proc sql;
	create table output.flussi_eta_union1 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from output.flussi_eta_union1
	group by cuni,eta;
	quit;

	data output.flussi_eta_union2;
	set output.flussi_eta_union1 output.flussi_eta_3;
	run;

	proc sql;
	create table output.flussi_eta_union2 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from output.flussi_eta_union2
	group by cuni,eta;
	quit;

	data output.flussi_eta_union3;
	set output.flussi_eta_union2 output.flussi_eta_4;
	run;

	proc sql;
	create table output.flussi_eta_union3 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from output.flussi_eta_union3
	group by cuni,eta;
	quit;

	data output.flussi_eta_union4;
	set output.flussi_eta_union3 output.flussi_eta_5;
	run;

	proc sql;
	create table output.flussi_eta_union4 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from output.flussi_eta_union4
	group by cuni,eta;
	quit;

	data output.flussi_eta_union5;
	set output.flussi_eta_union4 output.flussi_eta_6;
	run;

	proc sql;
	create table output.flussi_eta_union5 as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from output.flussi_eta_union5
	group by cuni,eta;
	quit;

	proc sort data=output.flussi_eta_union5;
	by cuni contatti_ssn;
	run;

	data output.flussi_eta_fin;
	set output.flussi_eta_union5;
	by cuni;
	retain last_unique;
	if last.cuni;
	run;


	/*JOIN*/

data output.flussi_2019;
merge output.flussi_sesso_fin(in=a) output.flussi_eta_fin(in=b);
by cuni;
if a;
run;

	/*proc sql;
	create table output.flussi_2019 as
	select a.*, b.eta
	from output.flussi_sesso_fin as a  
	left join output.flussi_eta_fin as b
	on a.cuni=b.cuni;
	quit;*/

data output.flussi_2019_2;
merge output.flussi_2019(in=a) output.flussi_reg_fin(in=b);
by cuni;
if a;
run;



	/*proc sql;
	create table output.flussi_2019 as
	select a.*, c.cod_regione
	from output.flussi_2019 as a
	left join output.flussi_reg_fin as c
	on a.cuni=c.cuni;
	quit;*/

	/*costo totale*/
	proc sql;
	create table output.costo_totale_1 as 
	select cuni, sum(costo) as costo_totale
	from ( select cuni, costo from output.flussi_2019_wrk_1)
	group by cuni;
	quit;

	proc sql;
	create table output.costo_totale_2 as 
	select cuni, sum(costo) as costo_totale
	from ( select cuni, costo from output.flussi_2019_wrk_2)
	group by cuni;
	quit;

	proc sql;
	create table output.costo_totale_3 as 
	select cuni, sum(costo) as costo_totale
	from ( select cuni, costo from output.flussi_2019_wrk_3)
	group by cuni;
	quit;

	proc sql;
	create table output.costo_totale_4 as 
	select cuni, sum(costo) as costo_totale
	from ( select cuni, costo from output.flussi_2019_wrk_4)
	group by cuni;
	quit;

	proc sql;
	create table output.costo_totale_5 as 
	select cuni, sum(costo) as costo_totale
	from ( select cuni, costo from output.flussi_2019_wrk_5)
	group by cuni;
	quit;

	proc sql;
	create table output.costo_totale_6 as 
	select cuni, sum(costo) as costo_totale
	from ( select cuni, costo from output.flussi_2019_wrk_6)
	group by cuni;
	quit;


data output.costo_totale_7;
set output.costo_totale_1  output.costo_totale_2  output.costo_totale_3 
output.costo_totale_4  output.costo_totale_5 output.costo_totale_6;
run;

	proc sql;
	create table output.costo_totale as 
	select cuni, sum(costo_totale) as costo_totale
	from output.costo_totale_7
	group by cuni;
	quit;


data output.flussi_2019_3;
merge output.flussi_2019_2(in=a) output.costo_totale(in=b);
by cuni;
if a;
run;


data output.flussi_2019;
set  output.flussi_2019_3;
run;



	/*proc sql;
	create table output.flussi_2019 as 
	select  a.cuni, a.cod_regione, a.sesso, a.eta, b.costo_totale as costo, a.contatti_ssn
	from output.flussi_2019 as a
	left join output.costo_totale as b
	on a.cuni=b.cuni;
	quit;*/


/*le altre macro*/



%info(data_in=output.flussi_2019, data_out=info_input);
%crea_dati_camp(dt_in=output.campionamento_semplice_4mln, dt_out=output.campione_flussi_4mln);
%info(data_in=output.campione_flussi_4mln, data_out=info_cs_4mln);
%valuta_camp(info_dt_pop=output.info_input, info_dt_camp=output.info_cs_4mln, camp=_4mln);
