proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\Anagrafica_ESENTI.txt" new;
run;


OPTIONS MPRINT;
OPTIONS COMPRESS =YES;


/* libname dei dati iniziali di input, dati forniti dal ministero*/
libname  INPUT "T:\202204_modello\Input Stratificazione";
%let INPUT = INPUT;

/* La libname temporanea si chiamer "temp", al momento impostata sulla work*/
libname  TEMP "T:\202204_modello\Output camp\temp";
%let TEMP = TEMP;

/* La Libname che conterr le tabelle create la chiamo LIBRARY */
LIBNAME OUTPUT "T:\202204_modello\Output camp\output";
%let OUTPUT = OUTPUT;

libname camp "T:\202204_modello\Output camp";

%global check_indicatore;


%let dt_riferimento = "31122019";


/*CREAZIONE DELL'ANAGRAFICA ESENTI PARTENDO DAI FLUSSI AMBULATORIALE E FARMACEUTICA CONVENZIONATA*/


%macro crea_anagrafe_esenti (reg=);
/*estrae le coppie di id_anonimo ed esenzioni dai vari flussi*/
	proc sql;
	create table &temp..amb_ese_wrk_&reg as 
	select id_anonimo, 
			cod_ese,
			min(coalesce(dt_erogazione, mdy(1,1,anno)))as min_dt_amb format=ddmmyy8., 
			max(coalesce(dt_erogazione, mdy(12,31,anno)))as max_dt_amb format=ddmmyy8.
	from output.amb_&reg
	where cod_ese ne ""
	group by id_anonimo, cod_ese;
	quit;

	 /*ATTENZIONE: non abbiamo la data di acquisto ma abbiamo "la Data di erogazione dei medicinali"= data_erogazione*/
	proc sql;
	create table &temp..farma_ese_wrk_&reg as  
	select id_anonimo,
			cod_ese, 
			min(data_erogazione) as min_dt_farma format=ddmmyy8.,
			max(data_erogazione) as max_dt_farma format=ddmmyy8.
	from output.farma_&reg
	where cod_ese ne ""
	group by id_anonimo, cod_ese;
	quit;

	
/*unisce le coppie di id_anonimo e id_esenzione individuate nei vari flussi*/
	proc sql;
	create table &temp..esenzioni_wrk1_&reg as
		select a.id_anonimo, a.cod_ese  
		from &temp..amb_ese_wrk_&reg as a
	union 
		select b.id_anonimo, b.cod_ese 
		from &temp..farma_ese_wrk_&reg as b											
	;quit;

/*aggancia le colonne relative alle date*/
	proc sql;
	create table &temp..esenzioni_wrk2_&reg as 
	select a.*,
			min(b.min_dt_amb, c.min_dt_farma) as dt_inizio_godimento format=ddmmyy8.,
			max(b.max_dt_amb, c.max_dt_farma) as dt_fine_godimento format=ddmmyy8.
	from &temp..esenzioni_wrk1_&reg as a
	left join &temp..amb_ese_wrk_&reg as b
		on a.id_anonimo=b.id_anonimo and a.cod_ese=b.cod_ese
	left join &temp..farma_ese_wrk_&reg as c
		on a.id_anonimo=c.id_anonimo and a.cod_ese=c.cod_ese	
	;quit;

	proc sql;
	create table &output..esenzioni_&reg as 
					select a.* , b.cod_regione as reg /*ATTENAZIONE: qua la regione di erogazione o la regione di residenza, prima c'era "cod_res"*/
					from &temp..esenzioni_wrk2_&reg as a
					left join camp.Anagrafica_4mln as b
					on a.id_anonimo=b.id_anonimo;
	quit;

	/*proc delete data= &temp..amb_ese_wrk_&reg &temp..farma_ese_wrk_&reg &temp..esenzioni_wrk1_&reg &temp..esenzioni_wrk2_&reg;
	run;*/
%mend;
options mprint;

%crea_anagrafe_esenti (reg=010);
%crea_anagrafe_esenti (reg=020);
%crea_anagrafe_esenti (reg=030);
%crea_anagrafe_esenti (reg=041);
%crea_anagrafe_esenti (reg=042);
%crea_anagrafe_esenti (reg=050);
%crea_anagrafe_esenti (reg=060);
%crea_anagrafe_esenti (reg=070);
%crea_anagrafe_esenti (reg=080);
%crea_anagrafe_esenti (reg=090);
%crea_anagrafe_esenti (reg=100);
%crea_anagrafe_esenti (reg=110);
%crea_anagrafe_esenti (reg=120);
%crea_anagrafe_esenti (reg=130);
%crea_anagrafe_esenti (reg=140);
%crea_anagrafe_esenti (reg=150);
%crea_anagrafe_esenti (reg=160);
%crea_anagrafe_esenti (reg=170);
%crea_anagrafe_esenti (reg=180);
%crea_anagrafe_esenti (reg=190);
%crea_anagrafe_esenti (reg=200);


