libname input "T:\202204_modello\Output camp\temp";
libname area "T:\202204_modello\Input Stratificazione";


/*creazione tabella*/

data input.area_patologica;
set area.area_patologica;
run;

proc sql noprint;
	select distinct (id_patologia)
	into: PATsql2 separated by ', 0 as '	
from input.area_patologica;
run;
%put &PATsql2;

proc sql;
create table input.comb_1 as select *,0 as &PATsql2
from TEMP.COMBINAZIONI_FIN_TOTALE_ZERO
;quit;

data input.comb_1;
set input.comb_1;
if index(COMBINAZIONE,"CA01") > 0 then CA01=1;  
if index(COMBINAZIONE,"CA02") > 0 then CA02=1;  
if index(COMBINAZIONE,"CA03") > 0 then CA03=1;  
if index(COMBINAZIONE,"CA04") > 0 then CA04=1;  
if index(COMBINAZIONE,"CA05") > 0 then CA05=1;	
if index(COMBINAZIONE,"CA06") > 0 then CA06=1;	
if index(COMBINAZIONE,"CA07") > 0 then CA07=1;	
if index(COMBINAZIONE,"CA08") > 0 then CA08=1;	
if index(COMBINAZIONE,"CA09") > 0 then CA09=1;	
if index(COMBINAZIONE,"CA10") > 0 then CA10=1;	
if index(COMBINAZIONE,"CA11") > 0 then CA11=1;	
if index(COMBINAZIONE,"PS01") > 0 then PS01=1;	
if index(COMBINAZIONE,"PS02") > 0 then PS02=1;	
if index(COMBINAZIONE,"PS03") > 0 then PS03=1;	
if index(COMBINAZIONE,"PS04") > 0 then PS04=1;	
if index(COMBINAZIONE,"PS05") > 0 then PS05=1;	
if index(COMBINAZIONE,"PS06") > 0 then PS06=1;	
if index(COMBINAZIONE,"NF01") > 0 then NF01=1;	
if index(COMBINAZIONE,"NF02") > 0 then NF02=1;	
if index(COMBINAZIONE,"RE01") > 0 then RE01=1;	
if index(COMBINAZIONE,"RE02") > 0 then RE02=1;	
if index(COMBINAZIONE,"RE03") > 0 then RE03=1;	
if index(COMBINAZIONE,"RE04") > 0 then RE04=1;	
if index(COMBINAZIONE,"RE05") > 0 then RE05=1;	
if index(COMBINAZIONE,"RE06") > 0 then RE06=1;	
if index(COMBINAZIONE,"RE07") > 0 then RE07=1;	
if index(COMBINAZIONE,"RE08") > 0 then RE08=1;	
if index(COMBINAZIONE,"RE09") > 0 then RE09=1;	
if index(COMBINAZIONE,"RE10") > 0 then RE10=1;	
if index(COMBINAZIONE,"RE11") > 0 then RE11=1;	
if index(COMBINAZIONE,"EM01") > 0 then EM01=1;	
if index(COMBINAZIONE,"EM02") > 0 then EM02=1;	
if index(COMBINAZIONE,"EM03") > 0 then EM03=1;	
if index(COMBINAZIONE,"EM04") > 0 then EM04=1;	
if index(COMBINAZIONE,"EM05") > 0 then EM05=1;	
if index(COMBINAZIONE,"EM06") > 0 then EM06=1;	
if index(COMBINAZIONE,"EM07") > 0 then EM07=1;	
if index(COMBINAZIONE,"EM08") > 0 then EM08=1;	
if index(COMBINAZIONE,"EM09") > 0 then EM09=1;	
if index(COMBINAZIONE,"DI01") > 0 then DI01=1;	
if index(COMBINAZIONE,"DI02") > 0 then DI02=1;	
if index(COMBINAZIONE,"DI03") > 0 then DI03=1;	
if index(COMBINAZIONE,"DI04") > 0 then DI04=1;	
if index(COMBINAZIONE,"NU01") > 0 then NU01=1;	
if index(COMBINAZIONE,"NU02") > 0 then NU02=1;	
if index(COMBINAZIONE,"NU03") > 0 then NU03=1;	
if index(COMBINAZIONE,"NU04") > 0 then NU04=1;	
if index(COMBINAZIONE,"NU05") > 0 then NU05=1;	
if index(COMBINAZIONE,"NU06") > 0 then NU06=1;	
if index(COMBINAZIONE,"NU07") > 0 then NU07=1;	
if index(COMBINAZIONE,"NU08") > 0 then NU08=1;	
if index(COMBINAZIONE,"NU09") > 0 then NU09=1;	
if index(COMBINAZIONE,"NU10") > 0 then NU10=1;	
if index(COMBINAZIONE,"PN01") > 0 then PN01=1;	
if index(COMBINAZIONE,"PN02") > 0 then PN02=1;	
if index(COMBINAZIONE,"PN03") > 0 then PN03=1;	
if index(COMBINAZIONE,"PN04") > 0 then PN04=1;	
if index(COMBINAZIONE,"PN05") > 0 then PN05=1;	
if index(COMBINAZIONE,"ED01") > 0 then ED01=1;	
if index(COMBINAZIONE,"ED02") > 0 then ED02=1;	
if index(COMBINAZIONE,"ED03") > 0 then ED03=1;	
if index(COMBINAZIONE,"ED04") > 0 then ED04=1;	
if index(COMBINAZIONE,"ED05") > 0 then ED05=1;	
if index(COMBINAZIONE,"ED06") > 0 then ED06=1;	
if index(COMBINAZIONE,"ED07") > 0 then ED07=1;	
if index(COMBINAZIONE,"ED08") > 0 then ED08=1;	
if index(COMBINAZIONE,"ED09") > 0 then ED09=1;	
if index(COMBINAZIONE,"ED10") > 0 then ED10=1;	
if index(COMBINAZIONE,"ED11") > 0 then ED11=1;	
if index(COMBINAZIONE,"ED12") > 0 then ED12=1;	
if index(COMBINAZIONE,"ED13") > 0 then ED13=1;	
if index(COMBINAZIONE,"ED14") > 0 then ED14=1;	
if index(COMBINAZIONE,"ED15") > 0 then ED15=1;	
if index(COMBINAZIONE,"ON01") > 0 then ON01=1;	
if index(COMBINAZIONE,"ON02") > 0 then ON02=1;	
if index(COMBINAZIONE,"ON03") > 0 then ON03=1;	
if index(COMBINAZIONE,"ON04") > 0 then ON04=1;	
if index(COMBINAZIONE,"ON05") > 0 then ON05=1;
if index(COMBINAZIONE,"ON06") > 0 then ON06=1;
if index(COMBINAZIONE,"ON07") > 0 then ON07=1;
run;


/*perche facciamo questo pasaggio? */

/*
data input.comb_&reg;
rename N_PAT=N_PAT_INI;
set input.comb_&reg;
if CA03=1 or CA02=1 then CA10=1; else CA10=0;
if PN01=1 or PN03=1 then PN04=1; else PN04=0;
*if NP06=1 or NP07=1 then NP10=1;
*else NP10=0;*questo non  utilizzato;
*if AL01=1 or TR02=1 or AL00=1 then AL05=1;
* else AL05=0;*questo non  uttilizzato;
if RE01=1 or RE02=1 or RE03=1 or RE04=1 or RE05=1 or RE07=1 or RE10=1 then RE11=1; else RE11=0;
run;
* questo lo uso se mi dicono che il passaggio di prima si fa --< da modificare negli script l1-L5 , N_PAT a posto di N_PAT_INI; 

data input.comb_ricod;
set input.comb_ricod;
array VAR &PATsas;
N_PAT=0;
do over VAR;
   if VAR=1 then N_PAT=N_PAT+VAR;
end;
run;

NOTE: Variable NP06 is uninitialized.
NOTE: Variable NP07 is uninitialized.
NOTE: Variable AL01 is uninitialized.
NOTE: Variable TR02 is uninitialized.
NOTE: Variable AL00 is uninitialized.

*/

/*
*/
data input.comb_ricod;
set input.comb_1;
length reg_desc $15.;
if regione="010" then reg_desc="Piomonte"; 
if regione="020" then reg_desc="Valle d'Aosta";
if regione="030" then reg_desc="Lombardia";
if regione="041" then reg_desc="Bolzano";
if regione="042" then reg_desc="trento";
if regione="050" then reg_desc="Veneto";
if regione="060" then reg_desc="Friuli V.G.";
if regione="070" then reg_desc="Liguria";
if regione="080" then reg_desc="Emilia Romagna";
if regione="090" then reg_desc="Toscana";
if regione="100" then reg_desc="Umbria";
if regione="110" then reg_desc="Marche";
if regione="120" then reg_desc="Lazio";
if regione="130" then reg_desc="Abruzzo";
if regione="140" then reg_desc="Molise";
if regione="150" then reg_desc="Campania";
if regione="160" then reg_desc="Puglia";
if regione="170" then reg_desc="Basilicata";
if regione="180" then reg_desc="Calabria";
if regione="190" then reg_desc="Sicilia";
if regione="200" then reg_desc="Sardegna";
run;



*Numerosit� totale di persone con malattie croniche;
proc means data=input.comb_ricod sum;var freq;run;*1.873.561;


proc sql noprint;
	select distinct (id_patologia)
	into: PATsas separated by ' '	
from input.area_patologica;
run;
%put &PATsas;

proc sql noprint;
	select distinct (id_patologia)
	into: PATsql separated by ','	
from input.area_patologica;
run;
%put &PATsql;

proc sql noprint;
	select count(*)
	into: NPAT 
from input.area_patologica;
run;
%put &NPAT;






	proc sql;*CON VARIABILE COMBINAIONE = 220.474- SENZA VAR COMBINAZIONE=62.850;
	create table input.start_data as 
	select /*COMBINAZIONE,*/
		&PATsql,
		sum(FREQ) as FREQ,
		/*sum(NUM_DECESSI) as NUM_DECESSI,
		sum(NUM_RICOVERI) as NUM_RICOVERI,*/
/*COSTO MEDIO*/
		sum(SUM_VAL)/sum(FREQ) as COSTO_MEDIO,
		sum(SUM_VAL_AMB)/sum(FREQ) as COSTO_MEDIO_AMB,
		sum(SUM_RICOVERI)/sum(FREQ) as COSTO_MEDIO_RICOVERI,
		sum(SUM_COSTO_FARMA)/sum(FREQ) as COSTO_MEDIO_FARMA,
		sum(SUM_FARMA_TERR)/sum(FREQ) as COSTO_MEDIO_FARMA_TERR,
		sum(SUM_HOSPICE)/sum(FREQ) as COSTO_MEDIO_HOSPICE,
		sum(SUM_PS)/sum(FREQ) as COSTO_MEDIO_PS,
/*SOMMA*/
		sum(SUM_VAL) as SUM_VAL,	
		sum(SUM_VAL_AMB)as SUM_VAL_AMB,
		sum(SUM_RICOVERI)as SUM_VAL_RICOVERI,
		sum(SUM_COSTO_FARMA)as SUM_VAL_FARMA,
		sum(SUM_FARMA_TERR)as SUM_VAL_FARMA_TERR,
		sum(SUM_HOSPICE) as SUM_VAL_HOSPICE,
		sum(SUM_PS) as SUM_VAL_PS,
/*LOG*/
		sum(SUM_LOG_VAL) as SUM_LOG_VAL,
		sum(SUM_LOG_VAL_AMB) as SUM_LOG_VAL_AMB,
		sum(SUM_LOG_RICOVERI) as SUM_LOG_VAL_RICOVERI,
		sum(SUM_LOG_COSTO_FARMA) as SUM_LOG_VAL_FARMA,
		sum(SUM_LOG_FARMA_TERR) as SUM_LOG_VAL_FARMA_TERR,
		sum(SUM_LOG_HOSPICE) as SUM_LOG_VAL_HOSPICE,
		sum(SUM_LOG_PS) as SUM_LOG_VAL_PS,

/*SQUARE*/
		sum(SUM_SQUARE_VAL) as SUM_SQUARE_VAL,
		sum(SUM_SQUARE_VAL_AMB) as SUM_SQUARE_VAL_AMB,
		sum(SUM_SQUARE_RICOVERI) as SUM_SQUARE_VAL_RICOVERI,
		sum(SUM_SQUARE_COSTO_FARMA) as SUM_SQUARE_VAL_FARMA,
		sum(SUM_SQUARE_FARMA_TERR) as SUM_SQUARE_VAL_FARMA_TERR,
		sum(SUM_SQUARE_HOSPICE) as SUM_SQUARE_VAL_HOSPICE,
		sum(SUM_SQUARE_PS) as SUM_SQUARE_VAL_PS,
/*SQUARE-LOG*/
		sum(SUM_SQUARE_LOG_VAL) as SUM_SQUARE_LOG_VAL,
		sum(SUM_SQUARE_LOG_VAL_AMB) as SUM_SQUARE_LOG_VAL_AMB,
		sum(SUM_SQUARE_LOG_RICOVERI) as SUM_SQUARE_LOG_VAL_RICOVERI,
		sum(SUM_SQUARE_LOG_COSTO_FARMA) as SUM_SQUARE_LOG_VAL_FARMA,
		sum(SUM_SQUARE_LOG_FARMA_TERR) as SUM_SQUARE_LOG_VAL_FARMA_TERR,
		sum(SUM_SQUARE_LOG_HOSPICE) as SUM_SQUARE_LOG_VAL_HOSPICE,
		sum(SUM_SQUARE_LOG_PS) as SUM_SQUARE_LOG_VAL_PS,
		N_PAT
	from input.comb_ricod
	group by &PATsql, N_PAT;
	;quit;

	

	data INPUT.essential_data(keep= &PATsas FREQ COSTO_MEDIO SUM_VAL SUM_SQUARE_VAL N_PAT);
	set input.start_data;
	run;

	*Numerosit� totale di persone con malattie croniche;
	proc means data=INPUT.essential_data sum;var freq;run;*1.873.561;

	
