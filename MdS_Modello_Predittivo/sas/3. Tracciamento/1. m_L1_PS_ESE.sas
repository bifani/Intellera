/* LIVELLO 1 - RICOVERI DIAGNOSI */

OPTIONS MPRINT;
OPTIONS COMPRESS =YES;
proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\L1_PS_ESE.txt" new;
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
%macro L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=,reg=);

		PROC SQL;
		SELECT count(*) INTO :check_indicatore FROM input.CRIT_L1_ESE WHERE ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.
		;QUIT;

	%IF %eval(&check_indicatore. > 0) %THEN %DO;

			PROC SQL;
				CREATE TABLE temp.L1_PS_ESE_WRK_&reg  AS
					SELECT DISTINCT
						CRIT.ID_CLASSIFICAZIONE,
						CRIT.ID_INDICATORE,
						ESE.ID_ANONIMO,
						ESE.data_presa_incarico AS DT_INI_ESE
					FROM
						output.ps_&reg ESE,
						input.CRIT_L1_ESE CRIT,
						input.PROFONDITA_TEMPORALE T
					WHERE
						CRIT.ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.
						/*AND ESE.COD_REG=CRIT.COD_REG*/
						AND(
						(CRIT.FL_INI='0' AND compress(ESE.cod_ese_pagamento, ".")=CRIT.ID_ESE) OR
						(CRIT.FL_INI='1' AND substr(compress(ESE.cod_ese_pagamento,"."),1,length(CRIT.ID_ESE))=CRIT.ID_ESE))
						AND T.ID_INDICATORE=CRIT.ID_INDICATORE
						AND CRIT.ID_CLASSIFICAZIONE = T.ID_CLASSIFICAZIONE
						/*AND (missing(ESE.DT_ANNULLAMENTO) OR (&ID_CLASSIFICAZIONE=9 AND INPUT(ESE.DT_ANNULLAMENTO, ddmmyy8.)>INPUT(&dt_riferimento., ddmmyy8.)))*/
						AND (CRIT.FL_DT_INI_ESE='0' OR (
							CRIT.FL_DT_INI_ESE='1'
							AND (ESE.data_presa_incarico BETWEEN intnx("month",input(&DT_RIFERIMENTO.,ddmmyy8.), T.NM_PROFONDITA_A_ANNI*-12, 's')+1
								AND intnx("month",input(&DT_RIFERIMENTO.,ddmmyy8.), T.NM_PROFONDITA_DA_ANNI*-12, 's'))
													))
					;QUIT;

	%END;
%MEND;




%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=010);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=020);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=030);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=041);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=042);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=050);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=060);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=070);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=080);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=090);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=100);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=110);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=120);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=130);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=140);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=150);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=160);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=170);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=180);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=190);
%L1_PS_ESENZIONI(ID_CLASSIFICAZIONE=9,reg=200);
