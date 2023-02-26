options compress = yes;
*%let impact_th=2; *impostare la soglia di impatto accettabile;
*%let hedges_th=0.20; *impostare la soglia del test del hedges accettabile;
*%let rate_th=0.85;  *impostare la soglia del rapporto tra le medie ("mono" vs multi);
/*Soglie nuove*/
/*
%let impact_th=0.1; *impostare la soglia di impatto accettabile;
%let hedges_th=0.75; *impostare la soglia del test del hedges accettabile;
%let rate_th=0.67;  *impostare la soglia del rapporto tra le medie ("mono" vs multi);
*/
%let impact_th=1; *impostare la soglia di impatto accettabile;
%let hedges_th=0.27; *impostare la soglia del test del hedges accettabile;
%let rate_th=0.67;  *impostare la soglia del rapporto tra le medie ("mono" vs multi);
%let rate_th_l4=0.85;  *impostare la soglia del rapporto tra le medie ("mono" vs multi);


%global PATsas;
%global PATsql;
%global NPAT;
%global maximpact;
%global keep;
%global rate_multi;
%global maxrate;
%global NPAT_rate;

libname input "T:\202204_modello\Output camp\temp";
/*Inserire la cartella dove si vuole avere i risultati finali, creare cartella "output"
eseguendo il percorso: T:\202204_modello\Output camp\temp\ - creare cartella- */

/*inserire percorso*/
%let percorso=T:\202204_modello\Output camp\temp\output;

libname output "&&percorso.";

*start macrovariables creation*;
%macrovar_list_creation();

*preprocessing*;
*%ricod(data=input.comb); *questa macro mi aspetto di eliminarla in futuro e passare alla macro group gi� i dati con la codifica definitiva;
*%group(data=input.comb_ricod);
*%drop(data=start_data);

*starting analysis*;
%info_chronic(data=input.essential_data);
%info_mono(data=input.essential_data);
%impact(data=input.essential_data);
*ranking creation*;
%ranking(data=output.info_mono, var=mean_val);

*stratification*;
proc printto log="&&percorso.\stratificazione_ciclo.txt" new;
run;
%stratified(essential_data= input.essential_data, comb_data= input.comb_ricod);
%group_for_stratification(data=output.comb_str);
%stratification_analysis();

proc means data=output.analysis_stratification sum;var freq;run;*1.873.561;
