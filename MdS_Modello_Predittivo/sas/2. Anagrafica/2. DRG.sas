proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\DRG.txt" new;
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


/*L'aggiunta del valore economico al flusso SDO*/



%macro drg(reg=);

proc sql;
create table output.SDO_&reg._1 as select a.*,(a.dt_DIMISSIONE - a.DATA_RIC) as DIFF, b.soglia_A,b.ENTRO,b.TRASF,b.DIURNI_RO,b.OLTRE
from output.SDO_&reg as a left join input.CRIT_DRG as b
on a.id_drg=b.drg24
;quit;

data output.SDO_&reg;
set output.SDO_&reg._1;
diff_soglia= DIFF - SOGLIA_A;
if 1< DIFF <= SOGLIA_A then AM_VALORE_TOT_EURO=ENTRO;
if DIFF=1 then AM_VALORE_TOT_EURO=TRASF;
if DIFF=0 then AM_VALORE_TOT_EURO=DIURNI_RO;
if DIFF> SOGLIA_A then AM_VALORE_TOT_EURO=ENTRO + (diff_soglia*OLTRE);
drop diff_soglia;
run;

proc delete data=output.SDO_&reg._1;run;
%mend;


%drg(reg=010);
%drg(reg=020);
%drg(reg=030);
%drg(reg=041);
%drg(reg=042);
%drg(reg=050);
%drg(reg=060);
%drg(reg=070);
%drg(reg=080);
%drg(reg=090);
%drg(reg=100);
%drg(reg=110);
%drg(reg=120);
%drg(reg=130);
%drg(reg=140);
%drg(reg=150);
%drg(reg=160);
%drg(reg=170);
%drg(reg=180);
%drg(reg=190);
%drg(reg=200);


