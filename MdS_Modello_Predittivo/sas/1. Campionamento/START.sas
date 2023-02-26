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
proc printto log='T:\PROGRAMMI\Codice campionamento\log_08062022.txt' NEW;
run;
/*campionamento*/
%lista_cuna();
/*%nrow_data(data_in=output.lista_cuna, macro_par=N_pop);
%n_camp(var='proporzione');
%campionamento_semplice(dt_in=OUTPUT.lista_cuna, dt_out=output.campionamento_semplice_soglia);*/
%let n_tot=4000000;
%campionamento_semplice(dt_in=output.lista_cuna, dt_out=output.campionamento_semplice_4mln);
/*%campionamento_stratificato(data=input.stratification , sesso=FL_SESSO, eta=ID_ETA, regione=ID_REGIONE);*/

proc printto log='T:\PROGRAMMI\Codice campionamento\log_08062022.txt' NEW;
run;
/*valutazione dei risultati*/
%unisci_flussi();
%info(data_in=output.flussi_2019, data_out=info_input);
%crea_dati_camp(dt_in=output.campionamento_semplice_4mln, dt_out=output.campione_flussi_4mln);
%info(data_in=output.campione_flussi_4mln, data_out=info_cs_4mln);
%valuta_camp(info_dt_pop=output.info_input, info_dt_camp=output.info_cs_4mln, camp=_4mln);
/*%crea_dati_camp(dt_in=output.campionamento_semplice_soglie, dt_out=output.campione_flussi_soglie);
%info(data_in=output.campione_flussi_soglie, data_out=info_cs_soglie);
%valuta_camp(info_dt_pop=output.info_input, info_dt_camp=output.info_cs_soglie, camp=_soglie);*/
/*%plot(dt_pop=input.stratification, dt_camp=campionamento_semplice, var=classe_eta);
%plot(dt_pop=input.stratification, dt_camp=campionamento_semplice, var=fl_sesso);
*/
