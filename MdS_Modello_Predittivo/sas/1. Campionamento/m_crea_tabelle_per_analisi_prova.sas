%macro unisci_flussi();

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

	/* sesso */
	proc sql;
	create table output.flussi_sesso as
	select cuni, sesso , sum(contatti_ssn) as contatti_ssn
	from output.flussi_2019_wrk
	group by cuni,sesso;
	quit;

	proc sort data=output.flussi_sesso;
	by cuni contatti_ssn;
	run;

	data output.flussi_sesso_fin;
	set output.flussi_sesso;
	by cuni;
	retain last_unique;
	if last.cuni;
	run;

	/*proc delete data=output.flussi_sesso;
	run;*/
	/*reg */

	proc sql;
	create table output.flussi_reg as
	select cuni, cod_regione , sum(contatti_ssn) as contatti_ssn
	from  output.flussi_2019_wrk
	group by cuni,cod_regione;
	quit;

	proc sort data=output.flussi_reg;
	by cuni contatti_ssn;
	run;

	data output.flussi_reg_fin;
	set output.flussi_reg;
	by cuni;
	retain last_unique;
	if last.cuni;
	run;
	
	/*proc delete data=output.flussi_reg;
	run;*/

	/*eta */

	proc sql;
	create table output.flussi_eta as
	select cuni, eta , sum(contatti_ssn) as contatti_ssn
	from output.flussi_2019_wrk
	group by cuni,eta;
	quit;

	proc sort data=output.flussi_eta;
	by cuni contatti_ssn;
	run;

	data output.flussi_eta_fin;
	set output.flussi_eta;
	by cuni;
	retain last_unique;
	if last.cuni;
	run;

	/*proc delete data=output.flussi_eta;
	run;*/

	proc sql;
	create table output.flussi_2019 as
	select a.*, b.eta
	from output.flussi_sesso_fin as a  
	left join output.flussi_eta_fin as b
	on a.cuni=b.cuni;
	quit;

	proc sql;
	create table output.flussi_2019 as
	select a.*, c.cod_regione
	from output.flussi_2019 as a
	left join output.flussi_reg_fin as c
	on a.cuni=c.cuni;
	quit;

	/*proc delete data=output.flussi_sesso_fin output.flussi_reg_fin output.flussi_eta_fin;
	run;*/

	/*costo totale*/
	proc sql;
	create table output.costo_totale as 
	select cuni, sum(costo) as costo_totale
	from ( select cuni, costo from output.flussi_2019_wrk)
	group by cuni;
	quit;



	proc sql;
	create table output.flussi_2019 as 
	select  a.cuni, a.cod_regione, a.sesso, a.eta, b.costo_totale as costo, a.contatti_ssn
	from output.flussi_2019 as a
	left join output.costo_totale as b
	on a.cuni=b.cuni;
	quit;

	/*proc delete data=output.costo_totale output.flussi_2019_wrk;
	run;*/


%mend;
*******************************;
%macro crea_dati_camp(dt_in=, dt_out=);

	proc sql;
	create table &dt_out as
	select a.cuni, b.cod_regione, b.sesso, b.eta, b.costo, b.contatti_ssn
	from &dt_in as a
	left join output.flussi_2019 as b
	on a.cuni=b.cuni;
	quit;

%mend;



/* TOTALE COSTO */


%macro tot_costo(input=, output=);
proc sql;
create table &output as
select cod_regione,eta,
case when  ETA ge 85 and  ETA ne 999 then '>=85'
				when  ETA ge  0 and  ETA lt 15 then '<=14'
				when  ETA ge 15 and  ETA lt 45 then '15-44'
				when  ETA ge 45 and  ETA lt 65 then '45-64'
				when  ETA ge 65 and  ETA lt 75 then '65-74'
				when  ETA ge 75 and  ETA lt 85 then '75-84'
				else 'NA' end as classe_eta ,
sum(costo) as tot_costo
from &input
group by cod_regione, eta, classe_eta;
quit;
%mend;

/*
%tot_costo(input=input.fmc_2019, output=output.tot_costo_fmc);
%tot_costo(input=input.amb_2019, output=output.tot_costo_amb);
*/
