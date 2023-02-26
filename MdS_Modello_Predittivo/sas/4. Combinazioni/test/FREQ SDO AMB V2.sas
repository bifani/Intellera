proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\FREQ_AMB_PRETAZ_VERSIONE2.txt" new;
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

/*AMBULATORIALE*/

/*INIZIO MACRO*/
%macro PATOLOGIA(PAT=);

%macro FREQ(reg=);

*Tabella dei COSTI TOTALI;
proc sql;
create table temp.AMB_01_&PAT._&reg as select a.*,b.flag_comorbidita
from (SELECT * from temp.ID_PAT_CRONICI_&reg where ID_PATOLOGIA="&&PAT.") as a 
left join (select * from temp.COMORBIDITA where regione="&&reg.") as b
on a.id_anonimo=b.id_anonimo 
;quit;


    *COSTO PER ETA CRONICI;

/*OUTPUT MONOPATOLOGICI*/

		data temp.AMB_02_&PAT._&reg;
		set temp.AMB_01_&PAT._&reg;
		where flag_comorbidita=0;
		run;

	*PRESTAZIONI AMB- DRG;
		PROC SQL;
		CREATE TABLE temp.AMB_03_&PAT._&reg as select id_anonimo,compress(ID_PRESTAZIONE,".") as ID_PRESTAZIONE,anno
		from output.AMB_&reg
		WHERE id_anonimo IN (SELECT ID_ANONIMO FROM temp.AMB_02_&PAT._&reg) and anno in (2017,2018,2019)
		;QUIT;


		PROC SQL;
		CREATE TABLE temp.AMB_&PAT._&reg as select  "&&PAT." as ID_PATOLOGIA,"&&reg." as REGIONE,
		ID_PRESTAZIONE,count(ID_ANONIMO) as freq , count(distinct ID_ANONIMO) as persone
		from temp.AMB_03_&PAT._&reg
		GROUP BY ID_PRESTAZIONE
		;QUIT;

		proc delete data=temp.AMB_01_&PAT._&reg temp.AMB_02_&PAT._&reg temp.AMB_03_&PAT._&reg;run;

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

data temp.AMB_&PAT;
set temp.AMB_&PAT._010 temp.AMB_&PAT._020 temp.AMB_&PAT._030 temp.AMB_&PAT._041
temp.AMB_&PAT._042 temp.AMB_&PAT._050 temp.AMB_&PAT._060 temp.AMB_&PAT._070
temp.AMB_&PAT._080 temp.AMB_&PAT._090 temp.AMB_&PAT._100 temp.AMB_&PAT._110
temp.AMB_&PAT._120 temp.AMB_&PAT._130 temp.AMB_&PAT._140 temp.AMB_&PAT._150
temp.AMB_&PAT._160 temp.AMB_&PAT._170 temp.AMB_&PAT._180 temp.AMB_&PAT._190
temp.AMB_&PAT._200;
run;

PROC SQL;
CREATE TABLE temp.AMB_&PAT as select distinct ID_PATOLOGIA,ID_PRESTAZIONE,sum(freq) as freq,
sum(distinct persone) as persone 
from temp.AMB_&PAT
GROUP BY ID_PRESTAZIONE
;QUIT;

/*AGGIUNGO L'AREA PATOLOGICA*/

PROC SQL;
CREATE TABLE temp.AMB_&PAT as select a.* ,b.DESC_PATOLOGIA, b.ID_AREA_PATOLOGICA,b.DESC_AREA_PATOLOGICA
from temp.AMB_&PAT as a left join (select * FROM input.area_patologica where id_classificazione =9) AS B 
on a.id_patologia=b.id_patologia
;QUIT;

proc sort data=temp.AMB_&PAT;by descending freq ;run;

proc export data = temp.AMB_&PAT
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\AMB\FREQ_AMB_&&PAT..xlsx"
	dbms = xlsx
	replace;
run;


proc delete data=temp.AMB_&PAT._010 temp.AMB_&PAT._020 temp.AMB_&PAT._030 
temp.AMB_&PAT._041
temp.AMB_&PAT._042 temp.AMB_&PAT._050 temp.AMB_&PAT._060 temp.AMB_&PAT._070
temp.AMB_&PAT._080 temp.AMB_&PAT._090 temp.AMB_&PAT._100 temp.AMB_&PAT._110
temp.AMB_&PAT._120 temp.AMB_&PAT._130 temp.AMB_&PAT._140 temp.AMB_&PAT._150
temp.AMB_&PAT._160 temp.AMB_&PAT._170 temp.AMB_&PAT._180 temp.AMB_&PAT._190
temp.AMB_&PAT._200
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

data temp.AREA_AMB;
set 
temp.AMB_CA01
temp.AMB_CA02
temp.AMB_CA03
temp.AMB_CA04
temp.AMB_CA05
temp.AMB_CA06
temp.AMB_CA07
temp.AMB_CA08
temp.AMB_CA09
temp.AMB_CA10
temp.AMB_CA11
temp.AMB_PS01
temp.AMB_PS02
temp.AMB_PS03
temp.AMB_PS04
temp.AMB_PS05
temp.AMB_PS06
temp.AMB_NF01
temp.AMB_NF02
temp.AMB_RE01
temp.AMB_RE02
temp.AMB_RE03
temp.AMB_RE04
temp.AMB_RE05
temp.AMB_RE06
temp.AMB_RE07
temp.AMB_RE08
temp.AMB_RE09
temp.AMB_RE10
temp.AMB_RE11
temp.AMB_EM01
temp.AMB_EM02
temp.AMB_EM03
temp.AMB_EM04
temp.AMB_EM05
temp.AMB_EM06
temp.AMB_EM07
temp.AMB_EM08
temp.AMB_EM09
temp.AMB_DI01
temp.AMB_DI02
temp.AMB_DI03
temp.AMB_DI04
temp.AMB_NU01
temp.AMB_NU02
temp.AMB_NU03
temp.AMB_NU04
temp.AMB_NU05
temp.AMB_NU06
temp.AMB_NU07
temp.AMB_NU08
temp.AMB_NU09
temp.AMB_NU10
temp.AMB_PN01
temp.AMB_PN02
temp.AMB_PN03
temp.AMB_PN04
temp.AMB_PN05
temp.AMB_ED01
temp.AMB_ED02
temp.AMB_ED03
temp.AMB_ED04
temp.AMB_ED05
temp.AMB_ED06
temp.AMB_ED07
temp.AMB_ED08
temp.AMB_ED09
temp.AMB_ED10
temp.AMB_ED11
temp.AMB_ED12
temp.AMB_ED13
temp.AMB_ED14
temp.AMB_ED15
temp.AMB_ON01
temp.AMB_ON02
temp.AMB_ON03
temp.AMB_ON04
temp.AMB_ON05
temp.AMB_ON06
temp.AMB_ON07
;run;


proc export data = temp.AREA_AMB
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\AMB\FREQ_AMB.xlsx"
	dbms = xlsx
	replace;
run;
