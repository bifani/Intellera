OPTIONS MPRINT;
OPTIONS COMPRESS =YES;
proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\L1_sdo_drg.txt" new;
run;

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




%macro L1_SDO_DRG(ID_CLASSIFICAZIONE=,reg=);

	PROC SQL;
		SELECT count(*) INTO :check_indicatore FROM input.CRIT_L1_SDO_DRG WHERE ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.
	;QUIT;

	%IF %eval(&check_indicatore. > 0) %THEN %DO;

		PROC SQL;
			CREATE TABLE temp.L1_SDO_DRG_WRK_&reg AS
				SELECT
						CRIT.ID_CLASSIFICAZIONE,
						CRIT.ID_INDICATORE,
						RIC.ID_ANONIMO,
						sum(1) AS QTA/*, ---> 20220718 --> Il valore lo tolgo perch� non presente nel flusso al momento
						sum(RIC.AM_VALORE_TOT_EURO) AS VAL*/
					FROM
						input.CRIT_L1_SDO_DRG CRIT,
						input.PROFONDITA_TEMPORALE C,
						output.SDO_&reg RIC
					WHERE
						CRIT.ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE.
						AND CRIT.ID_CLASSIFICAZIONE=C.ID_CLASSIFICAZIONE
						AND CRIT.ID_INDICATORE=C.ID_INDICATORE
						AND(
						(CRIT.FL_INI='0' AND compress(RIC.ID_DRG, ".")=CRIT.ID_DRG) OR
						(CRIT.FL_INI='1' AND substr(compress(RIC.ID_DRG,"."),1,length(CRIT.ID_DRG))=CRIT.ID_DRG))
						AND RIC.DT_DIMISSIONE BETWEEN intnx("month",input(&DT_RIFERIMENTO.,ddmmyy8.), C.NM_PROFONDITA_A_ANNI*-12 ,'s')+1
							AND intnx("month",input(&DT_RIFERIMENTO.,ddmmyy8.), C.NM_PROFONDITA_DA_ANNI*-12, 's')
					GROUP BY
						CRIT.ID_CLASSIFICAZIONE,
						CRIT.ID_INDICATORE,
						RIC.ID_ANONIMO
			;QUIT;
	%END;
%MEND;

/* prova macro
*/
/*
%let dt_riferimento = "31122018";
%L1_SDO_DRG(ID_CLASSIFICAZIONE = 9);
*/


%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=010);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=020);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=030);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=041);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=042);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=050);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=060);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=070);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=080);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=090);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=100);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=110);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=120);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=130);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=140);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=150);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=160);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=170);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=180);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=190);
%L1_SDO_DRG(ID_CLASSIFICAZIONE=9,reg=200);
