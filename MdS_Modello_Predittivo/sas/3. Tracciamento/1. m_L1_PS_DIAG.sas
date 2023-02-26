/* LIVELLO 1 - RICOVERI DIAGNOSI */

OPTIONS MPRINT;
OPTIONS COMPRESS =YES;
proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\L1_PS_DIAG.txt" new;
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

%macro L1_PS_DIAG(ID_CLASSIFICAZIONE=, reg=);
	PROC SQL;
		SELECT count(*) INTO :check_indicatore FROM input.CRIT_L1_SDO_DIAG WHERE ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.
	;QUIT;

	%IF %eval(&check_indicatore. > 0) %THEN %DO;
	********** PROFONDITA TEMPORALE ********************;
		PROC SQL;
				CREATE TABLE &temp..CRIT_L1_DIAG_PROFONDITA AS
					SELECT
						CRIT.ID_CLASSIFICAZIONE,
						CRIT.ID_INDICATORE,
						CRIT.FL_DIAG_PRIM,
						CRIT.ID_DIAG,
						FL_INI,
						LENGTH(ID_DIAG) AS LEN_DIAG,
						intnx("month",input(&DT_RIFERIMENTO.,ddmmyy10.), PT.NM_PROFONDITA_A_ANNI*-12, 's')+1  AS DIMISS_INI format ddmmyy10.,
						intnx("month",input(&DT_RIFERIMENTO.,ddmmyy10.), PT.NM_PROFONDITA_DA_ANNI*-12, 's') AS DIMISS_FINE format ddmmyy10.
					FROM
						input.CRIT_L1_SDO_DIAG CRIT
						JOIN input.PROFONDITA_TEMPORALE PT
							ON CRIT.ID_CLASSIFICAZIONE=PT.ID_CLASSIFICAZIONE AND CRIT.ID_INDICATORE=PT.ID_INDICATORE
					WHERE
					CRIT.ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE.
			;QUIT;

	********** RICOVERI *******************;
		PROC SQL;
			CREATE TABLE &TEMP..TMP_L1_DIAG_02_&reg AS
				SELECT
					T.ID_CLASSIFICAZIONE,
					T.ID_INDICATORE,
					RIC_F.ID_ANONIMO,
					RIC_F.data_dimiss
				FROM
					output.PS_&reg RIC_F,
					(SELECT * FROM &temp..CRIT_L1_DIAG_PROFONDITA WHERE FL_DIAG_PRIM='1' AND ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE.) T
				WHERE
					(T.FL_INI='0' AND compress(RIC_F.id_diag_prim, ".")=T.ID_DIAG) OR
					(T.FL_INI='1' AND substr(compress(RIC_F.id_diag_prim,"."),1,T.LEN_DIAG)=T.ID_DIAG)
					AND RIC_F.data_dimiss BETWEEN T.DIMISS_INI AND T.DIMISS_FINE
			UNION ALL
				SELECT
					T.ID_CLASSIFICAZIONE,
					T.ID_INDICATORE,
					RIC_F.ID_ANONIMO,
					RIC_F.data_dimiss
				FROM
					output.PS_&reg RIC_F,
					(SELECT * FROM temp.CRIT_L1_DIAG_PROFONDITA WHERE FL_DIAG_PRIM='0' AND ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE.) T
				WHERE
					(T.FL_INI='0' AND(
							compress(RIC_F.id_diag_prim, ".")=T.ID_DIAG  ))
					OR(T.FL_INI='1' AND(
							substr(compress(RIC_F.id_diag_prim,"."),1,T.LEN_DIAG)=T.ID_DIAG  ))
					AND RIC_F.data_dimiss BETWEEN T.DIMISS_INI AND T.DIMISS_FINE
		;QUIT;


		PROC SQL;
			CREATE TABLE TEMP.L1_PS_DIAG_WRK_&reg AS
				SELECT
					ID_CLASSIFICAZIONE,
					ID_INDICATORE,
					ID_ANONIMO,
					COUNT(*) AS QTA
				FROM TEMP.TMP_L1_DIAG_02_&reg
				GROUP BY
					ID_CLASSIFICAZIONE,
					ID_INDICATORE,
					ID_ANONIMO
		;QUIT;

		proc delete DATA= TEMP.CRIT_L1_DIAG_PROFONDITA &TEMP..TMP_L1_DIAG_02_&reg;
		run;

	%END;
%MEND;


%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=010);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=020);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=030);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=041);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=042);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=050);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=060);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=070);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=080);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=090);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=100);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=110);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=120);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=130);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=140);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=150);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=160);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=170);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=180);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=190);
%L1_PS_DIAG(ID_CLASSIFICAZIONE=9,reg=200);
