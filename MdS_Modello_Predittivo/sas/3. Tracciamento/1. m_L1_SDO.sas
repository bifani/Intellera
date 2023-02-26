/* LIVELLO 1 - RICOVERI DIAGNOSI */

OPTIONS MPRINT;
OPTIONS COMPRESS =YES;
proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\L1_SDO.txt" new;
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

%macro L1_SDO(ID_CLASSIFICAZIONE=,reg=); /*-> correlazione tra le diagnosi principali e drg*/
	PROC SQL;
		SELECT count(*) INTO :check_indicatore FROM input.CRIT_L1_SDO WHERE ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.
	;QUIT;

	%IF %eval(&check_indicatore. > 0) %THEN %DO;

		PROC SQL;
				CREATE TABLE temp.CRIT_L1_SDO_PROFONDITA AS
					SELECT
						CRIT.ID_CLASSIFICAZIONE,
						CRIT.ID_INDICATORE,
						CRIT.FL_DIAG_PRIM,
						CRIT.ID_DIAG,
						CRIT.FL_INI_DIAG,
						CRIT.ID_DRG,
						CRIT.FL_INI_DRG,
						LENGTH(CRIT.ID_DIAG) AS LEN_DIAG,
						LENGTH(CRIT.ID_DRG) AS LEN_DRG,
						intnx("month",input(&DT_RIFERIMENTO.,ddmmyy10.), PT.NM_PROFONDITA_A_ANNI*-12, 's')+1  AS DIMISS_INI format ddmmyy10.,
						intnx("month",input(&DT_RIFERIMENTO.,ddmmyy10.), PT.NM_PROFONDITA_DA_ANNI*-12, 's') AS DIMISS_FINE format ddmmyy10.
					FROM
						input.CRIT_L1_SDO CRIT
						JOIN input.PROFONDITA_TEMPORALE PT
							ON CRIT.ID_CLASSIFICAZIONE=PT.ID_CLASSIFICAZIONE AND CRIT.ID_INDICATORE=PT.ID_INDICATORE
					WHERE
					CRIT.ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE.
		;QUIT;

		*** FILTRO RICOVERI CON DIAGNOSI NELLA LISTA *****;

		PROC SQL;
				CREATE TABLE temp.TMP_L1_SDO_01_&reg AS
					SELECT 	ID_ANONIMO,
							ID_DIAG_PRIM,
							ID_DIAG_SEC_1,
							ID_DIAG_SEC_2,
							ID_DIAG_SEC_3,
							ID_DIAG_SEC_4,
							ID_DIAG_SEC_5,
							RIC.ID_DRG,
							DT_DIMISSIONE
					FROM output.SDO_&reg RIC,
						(SELECT DISTINCT ID_DIAG, LEN_DIAG, FL_INI_DIAG FROM temp.CRIT_L1_SDO_PROFONDITA WHERE ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.) T
					WHERE
						(T.FL_INI_DIAG='0' AND(
							compress(RIC.ID_DIAG_PRIM, ".")=T.ID_DIAG OR
							compress(RIC.ID_DIAG_SEC_1, ".")=T.ID_DIAG OR
							compress(RIC.ID_DIAG_SEC_2, ".")=T.ID_DIAG OR
							compress(RIC.ID_DIAG_SEC_3, ".")=T.ID_DIAG OR
							compress(RIC.ID_DIAG_SEC_4, ".")=T.ID_DIAG OR
							compress(RIC.ID_DIAG_SEC_5, ".")=T.ID_DIAG))
						OR (T.FL_INI_DIAG='1' AND(
							substr(compress(RIC.ID_DIAG_PRIM,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC.ID_DIAG_SEC_1,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC.ID_DIAG_SEC_2,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC.ID_DIAG_SEC_3,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC.ID_DIAG_SEC_4,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC.ID_DIAG_SEC_5,"."),1,T.LEN_DIAG)=T.ID_DIAG))
			;QUIT;

		*** SDO ***********************************;
		PROC SQL;
			CREATE TABLE temp.L1_SDO_WRK_&reg AS
				SELECT
					T.ID_CLASSIFICAZIONE,
					T.ID_INDICATORE,
					RIC_F.ID_ANONIMO
				FROM
					temp.TMP_L1_SDO_01_&reg RIC_F,
					(SELECT * FROM temp.CRIT_L1_SDO_PROFONDITA WHERE FL_DIAG_PRIM='1' AND ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE.) T
				WHERE
					(T.FL_INI_DIAG='0' AND compress(RIC_F.ID_DIAG_PRIM, ".")=T.ID_DIAG) OR
					(T.FL_INI_DIAG='1' AND substr(compress(RIC_F.ID_DIAG_PRIM,"."),1,T.LEN_DIAG)=T.ID_DIAG)
					AND (
						(T.FL_INI_DRG='0' AND compress(RIC_F.ID_DRG, ".")=T.ID_DRG)OR
						(T.FL_INI_DRG='1' AND substr(compress(RIC_F.ID_DRG,"."),1,T.LEN_DRG)=T.ID_DRG))
					AND RIC_F.DT_DIMISSIONE BETWEEN T.DIMISS_INI AND T.DIMISS_FINE
			UNION ALL
				SELECT
					T.ID_CLASSIFICAZIONE,
					T.ID_INDICATORE,
					RIC_F.ID_ANONIMO
				FROM
					temp.TMP_L1_SDO_01_&reg RIC_F,
					(SELECT * FROM temp.CRIT_L1_SDO_PROFONDITA WHERE FL_DIAG_PRIM='0' AND ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE.) T
				WHERE
							(T.FL_INI_DIAG='0' AND(
							compress(RIC_F.ID_DIAG_PRIM, ".")=T.ID_DIAG OR
							compress(RIC_F.ID_DIAG_SEC_1, ".")=T.ID_DIAG OR
							compress(RIC_F.ID_DIAG_SEC_2, ".")=T.ID_DIAG OR
							compress(RIC_F.ID_DIAG_SEC_3, ".")=T.ID_DIAG OR
							compress(RIC_F.ID_DIAG_SEC_4, ".")=T.ID_DIAG OR
							compress(RIC_F.ID_DIAG_SEC_5, ".")=T.ID_DIAG ))
					OR(T.FL_INI_DIAG='1' AND(
							substr(compress(RIC_F.ID_DIAG_PRIM,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC_F.ID_DIAG_SEC_1,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC_F.ID_DIAG_SEC_2,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC_F.ID_DIAG_SEC_3,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC_F.ID_DIAG_SEC_4,"."),1,T.LEN_DIAG)=T.ID_DIAG OR
							substr(compress(RIC_F.ID_DIAG_SEC_5,"."),1,T.LEN_DIAG)=T.ID_DIAG ))
					AND (
						(T.FL_INI_DRG='0' AND compress(RIC_F.ID_DRG, ".")=T.ID_DRG) OR
						(T.FL_INI_DRG='1' AND substr(compress(RIC_F.ID_DRG,"."),1,T.LEN_DRG)=T.ID_DRG))
					AND RIC_F.DT_DIMISSIONE BETWEEN T.DIMISS_INI AND T.DIMISS_FINE
		;QUIT;

		proc delete data=TEMP.CRIT_L1_SDO_PROFONDITA temp.TMP_L1_SDO_01_&reg;
		run;

	%END;
%MEND;


/* prova macro
*/
/*
%let dt_riferimento = "31122018";
%L1_SDO(ID_CLASSIFICAZIONE =9);
*/


%L1_SDO(reg=010,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=020,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=030,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=041,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=042,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=050,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=060,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=070,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=080,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=090,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=100,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=110,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=120,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=130,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=140,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=150,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=160,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=170,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=180,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=190,ID_CLASSIFICAZIONE =9);
%L1_SDO(reg=200,ID_CLASSIFICAZIONE =9);
