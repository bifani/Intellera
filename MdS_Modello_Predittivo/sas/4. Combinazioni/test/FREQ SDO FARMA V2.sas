proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\FREQ_FARMA_ATC_VERSIONE2.txt" new;
run;

OPTIONS MPRINT;
OPTIONS COMPRESS =YES;

/* libname dei dati iniziali di input, dati forniti dal ministero*/
libname  INPUT "T:\202204_modello\Input Stratificazione";
%let INPUT = INPUT;

/* La libname temporanea si chiamer� "temp", al momento impostata sulla work*/
libname  TEMP "T:\202204_modello\Output camp\temp";
%let TEMP = TEMP;

/* La Libname che conterr� le tabelle create la chiamo LIBRARY */
LIBNAME OUTPUT "T:\202204_modello\Output camp\output";
%let OUTPUT = OUTPUT;

libname camp "T:\202204_modello\Output camp";

%global check_indicatore;

%let dt_riferimento = "31122019";

libname camp "T:\202204_modello\Output camp";

/*INIZIO MACRO*/
%macro PATOLOGIA(PAT=);

%macro FREQ(reg=);

*Tabella dei COSTI TOTALI;
proc sql;
create table temp.FARMA_01_&PAT._&reg as select a.*, b.flag_comorbidita
from (SELECT * from temp.ID_PAT_CRONICI_&reg where ID_PATOLOGIA="&&PAT.") as a 
left join (select * from temp.COMORBIDITA where regione="&&reg.") as b
on a.id_anonimo=b.id_anonimo 
;quit;



/*OUTPUT MONOPATOLOGICI*/

		data temp.FARMA_02_&PAT._&reg;
		set temp.FARMA_01_&PAT._&reg;
		where flag_comorbidita=0;
		run;

	*PRESTAZIONI FARMA- DRG;
		PROC SQL;
		CREATE TABLE temp.FARMA_03_&PAT._&reg as select anno, cod_ricetta, data_erogazione, num_farma_ric ,cod_aic,
		quantita, costo_servizio, costo_acquisto, id_anonimo
		from output.FARMA_&reg
		WHERE id_anonimo IN (SELECT ID_ANONIMO FROM temp.FARMA_02_&PAT._&reg) and anno in (2017,2018,2019)
		;QUIT;


		
		/*DA FARE ATTENZIONE!*/

		data temp.farma_&reg._a temp.farma_&reg._b;
		set temp.FARMA_03_&PAT._&reg;
		if 0<=_n_<=20000000 then output temp.farma_&reg._a;
		if _n_  >20000000 then output temp.farma_&reg._b;
		run;


		proc sort  data=temp.farma_&reg._a;by cod_aic;run;
		proc sort  data=temp.farma_&reg._b;by cod_aic;run;

		data temp.farma_&reg._sort;
		merge temp.farma_&reg._a  temp.farma_&reg._b;
		by cod_aic;
		run;

		data temp.ddd_last_sort;
		set input.ddd_last;
		keep cod_aic atc;
		rename atc=id_atc;
		run;

		data temp.far_&reg._atc;
		merge temp.farma_&reg._sort (in = a) temp.ddd_last_sort (in = b);
		by cod_aic;
		if a;
		run;

	/*filtro soltanto gli atc presenti dentro ai criteri traccianti*/
  
	proc sql;
		create table temp.atc_list as select 
			distinct id_atc 
			from input.crit_l1_far
			where id_classificazione = 9;
	quit;

	proc sql;
		create table temp.FARMA_03_&PAT._&reg as select *
			from temp.far_&reg._atc
			where id_atc in (select id_atc from temp.atc_list);
	quit;



		PROC SQL;
		CREATE TABLE temp.FARMA_&PAT._&reg as select  "&&PAT." as ID_PATOLOGIA,"&&reg." as REGIONE,
		id_atc,count(ID_ANONIMO) as freq , count(distinct ID_ANONIMO) as persone
		from temp.FARMA_03_&PAT._&reg
		GROUP BY id_atc
		;QUIT;

		proc delete data=temp.FARMA_01_&PAT._&reg temp.FARMA_02_&PAT._&reg temp.FARMA_03_&PAT._&reg
		temp.farma_&reg._a temp.farma_&reg._b temp.farma_&reg._sort temp.ddd_last_sort temp.far_&reg._atc
		;run;

%mend;


%FREQ(reg=010);
%FREQ(reg=020);
%FREQ(reg=030);
%FREQ(reg=041);
%FREQ(reg=042);
%FREQ(reg=050);
%FREQ(reg=060);
%FREQ(reg=070);
%FREQ(reg=080);
%FREQ(reg=090);
%FREQ(reg=100);
%FREQ(reg=110);
%FREQ(reg=120);
%FREQ(reg=130);
%FREQ(reg=140);
%FREQ(reg=150);
%FREQ(reg=160);
%FREQ(reg=170);
%FREQ(reg=180);
%FREQ(reg=190);
%FREQ(reg=200);

data temp.FARMA_&PAT;
set temp.FARMA_&PAT._010 temp.FARMA_&PAT._020 temp.FARMA_&PAT._030 temp.FARMA_&PAT._041
temp.FARMA_&PAT._042 temp.FARMA_&PAT._050 temp.FARMA_&PAT._060 temp.FARMA_&PAT._070
temp.FARMA_&PAT._080 temp.FARMA_&PAT._090 temp.FARMA_&PAT._100 temp.FARMA_&PAT._110
temp.FARMA_&PAT._120 temp.FARMA_&PAT._130 temp.FARMA_&PAT._140 temp.FARMA_&PAT._150
temp.FARMA_&PAT._160 temp.FARMA_&PAT._170 temp.FARMA_&PAT._180 temp.FARMA_&PAT._190
temp.FARMA_&PAT._200;
run;

PROC SQL;
CREATE TABLE temp.FARMA_&PAT as select distinct ID_PATOLOGIA,ID_ATC,sum(freq) as freq , 
sum(distinct persone) as persone
from temp.FARMA_&PAT
GROUP BY ID_ATC
;QUIT;

/*AGGIUNGO L'AREA PATOLOGICA*/

PROC SQL;
CREATE TABLE temp.FARMA_&PAT as select a.* ,b.DESC_PATOLOGIA, b.ID_AREA_PATOLOGICA,b.DESC_AREA_PATOLOGICA
from temp.FARMA_&PAT as a left join (select * FROM input.area_patologica where id_classificazione =9) AS B 
on a.id_patologia=b.id_patologia
;QUIT;

proc sort data=temp.FARMA_&PAT;by descending freq;run;

proc export data = temp.FARMA_&PAT
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\FARMA\FREQ_FARMA_&&PAT..xlsx"
	dbms = xlsx
	replace;
run;

proc delete data=temp.FARMA_&PAT._010 temp.FARMA_&PAT._020 temp.FARMA_&PAT._030 temp.FARMA_&PAT._041
temp.FARMA_&PAT._042 temp.FARMA_&PAT._050 temp.FARMA_&PAT._060 temp.FARMA_&PAT._070
temp.FARMA_&PAT._080 temp.FARMA_&PAT._090 temp.FARMA_&PAT._100 temp.FARMA_&PAT._110
temp.FARMA_&PAT._120 temp.FARMA_&PAT._130 temp.FARMA_&PAT._140 temp.FARMA_&PAT._150
temp.FARMA_&PAT._160 temp.FARMA_&PAT._170 temp.FARMA_&PAT._180 temp.FARMA_&PAT._190
temp.FARMA_&PAT._200
;
run;

%mend;


%PATOLOGIA(PAT=CA01);
%PATOLOGIA(PAT=CA02);
%PATOLOGIA(PAT=CA03);
%PATOLOGIA(PAT=CA04);
%PATOLOGIA(PAT=CA05);
%PATOLOGIA(PAT=CA06);
%PATOLOGIA(PAT=CA07);
%PATOLOGIA(PAT=CA08);
%PATOLOGIA(PAT=CA09);
%PATOLOGIA(PAT=CA10);
%PATOLOGIA(PAT=CA11);
%PATOLOGIA(PAT=PS01);
%PATOLOGIA(PAT=PS02);
%PATOLOGIA(PAT=PS03);
%PATOLOGIA(PAT=PS04);
%PATOLOGIA(PAT=PS05);
%PATOLOGIA(PAT=PS06);
%PATOLOGIA(PAT=NF01);
%PATOLOGIA(PAT=NF02);
%PATOLOGIA(PAT=RE01);
%PATOLOGIA(PAT=RE02);
%PATOLOGIA(PAT=RE03);
%PATOLOGIA(PAT=RE04);
%PATOLOGIA(PAT=RE05);
%PATOLOGIA(PAT=RE06);
%PATOLOGIA(PAT=RE07);
%PATOLOGIA(PAT=RE08);
%PATOLOGIA(PAT=RE09);
%PATOLOGIA(PAT=RE10);
%PATOLOGIA(PAT=RE11);
%PATOLOGIA(PAT=EM01);
%PATOLOGIA(PAT=EM02);
%PATOLOGIA(PAT=EM03);
%PATOLOGIA(PAT=EM04);
%PATOLOGIA(PAT=EM05);
%PATOLOGIA(PAT=EM06);
%PATOLOGIA(PAT=EM07);
%PATOLOGIA(PAT=EM08);
%PATOLOGIA(PAT=EM09);
%PATOLOGIA(PAT=DI01);
%PATOLOGIA(PAT=DI02);
%PATOLOGIA(PAT=DI03);
%PATOLOGIA(PAT=DI04);
%PATOLOGIA(PAT=NU01);
%PATOLOGIA(PAT=NU02);
%PATOLOGIA(PAT=NU03);
%PATOLOGIA(PAT=NU04);
%PATOLOGIA(PAT=NU05);
%PATOLOGIA(PAT=NU06);
%PATOLOGIA(PAT=NU07);
%PATOLOGIA(PAT=NU08);
%PATOLOGIA(PAT=NU09);
%PATOLOGIA(PAT=NU10);
%PATOLOGIA(PAT=PN01);
%PATOLOGIA(PAT=PN02);
%PATOLOGIA(PAT=PN03);
%PATOLOGIA(PAT=PN04);
%PATOLOGIA(PAT=PN05);
%PATOLOGIA(PAT=ED01);
%PATOLOGIA(PAT=ED02);
%PATOLOGIA(PAT=ED03);
%PATOLOGIA(PAT=ED04);
%PATOLOGIA(PAT=ED05);
%PATOLOGIA(PAT=ED06);
%PATOLOGIA(PAT=ED07);
%PATOLOGIA(PAT=ED08);
%PATOLOGIA(PAT=ED09);
%PATOLOGIA(PAT=ED10);
%PATOLOGIA(PAT=ED11);
%PATOLOGIA(PAT=ED12);
%PATOLOGIA(PAT=ED13);
%PATOLOGIA(PAT=ED14);
%PATOLOGIA(PAT=ED15);
%PATOLOGIA(PAT=ON01);
%PATOLOGIA(PAT=ON02);
%PATOLOGIA(PAT=ON03);
%PATOLOGIA(PAT=ON04);
%PATOLOGIA(PAT=ON05);
%PATOLOGIA(PAT=ON06);
%PATOLOGIA(PAT=ON07);

/*TABELLA UNICA CON TUTTE LE PATOLOGIA*/

data temp.AREA_FARMA;
set 
temp.FARMA_CA01
temp.FARMA_CA02
temp.FARMA_CA03
temp.FARMA_CA04
temp.FARMA_CA05
temp.FARMA_CA06
temp.FARMA_CA07
temp.FARMA_CA08
temp.FARMA_CA09
temp.FARMA_CA10
temp.FARMA_CA11
temp.FARMA_PS01
temp.FARMA_PS02
temp.FARMA_PS03
temp.FARMA_PS04
temp.FARMA_PS05
temp.FARMA_PS06
temp.FARMA_NF01
temp.FARMA_NF02
temp.FARMA_RE01
temp.FARMA_RE02
temp.FARMA_RE03
temp.FARMA_RE04
temp.FARMA_RE05
temp.FARMA_RE06
temp.FARMA_RE07
temp.FARMA_RE08
temp.FARMA_RE09
temp.FARMA_RE10
temp.FARMA_RE11
temp.FARMA_EM01
temp.FARMA_EM02
temp.FARMA_EM03
temp.FARMA_EM04
temp.FARMA_EM05
temp.FARMA_EM06
temp.FARMA_EM07
temp.FARMA_EM08
temp.FARMA_EM09
temp.FARMA_DI01
temp.FARMA_DI02
temp.FARMA_DI03
temp.FARMA_DI04
temp.FARMA_NU01
temp.FARMA_NU02
temp.FARMA_NU03
temp.FARMA_NU04
temp.FARMA_NU05
temp.FARMA_NU06
temp.FARMA_NU07
temp.FARMA_NU08
temp.FARMA_NU09
temp.FARMA_NU10
temp.FARMA_PN01
temp.FARMA_PN02
temp.FARMA_PN03
temp.FARMA_PN04
temp.FARMA_PN05
temp.FARMA_ED01
temp.FARMA_ED02
temp.FARMA_ED03
temp.FARMA_ED04
temp.FARMA_ED05
temp.FARMA_ED06
temp.FARMA_ED07
temp.FARMA_ED08
temp.FARMA_ED09
temp.FARMA_ED10
temp.FARMA_ED11
temp.FARMA_ED12
temp.FARMA_ED13
temp.FARMA_ED14
temp.FARMA_ED15
temp.FARMA_ON01
temp.FARMA_ON02
temp.FARMA_ON03
temp.FARMA_ON04
temp.FARMA_ON05
temp.FARMA_ON06
temp.FARMA_ON07
;run;


proc export data = temp.AREA_FARMA
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\FARMA\FREQ_FARMA.xlsx"
	dbms = xlsx
	replace;
run;
