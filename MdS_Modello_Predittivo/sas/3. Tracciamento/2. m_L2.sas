/* LIVELLO 1 - RICOVERI DIAGNOSI */
proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\L2.txt" new;
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

/* La Libname che contiene la tabella anagrafica del campione di 4mln*/
libname camp "T:\202204_modello\Output camp";

%global check_indicatore;

%let dt_riferimento = "31122019";

/* LIVELLO 2 */
%macro L2(ID_CLASSIFICAZIONE=,reg=);

%LET PROFONDITA_FLUSSO=2;


PROC SQL;
	CREATE TABLE temp.L2_WRK_&reg (
	ID_CLASSIFICAZIONE num,
	ID_INDICATORE char(15),
	ID_ANONIMO num /*CHAR(100)*/)
;QUIT;

	/*** ESENZIONI ******************************/
/*
%if %sysfunc(exist(TEMP.L1_ESE_WRK_&reg)) %then %do;
			PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					A.ID_CLASSIFICAZIONE,
					A.ID_INDICATORE,
					A.ID_ANONIMO
				FROM
					TEMP.L1_ESE_WRK_&reg A
					JOIN (select id_anonimo, sesso, eta from camp.anagrafica_4mln where cod_regione = "&reg.") D ON A.ID_ANONIMO=D.ID_ANONIMO
					LEFT JOIN input.CRIT_L2_ESE B ON A.ID_CLASSIFICAZIONE=B.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=B.ID_INDICATORE
				WHERE
					(
						B.NM_ETA_INI_ESE_MAGGIORE IS missing
						OR (
							d.eta  > B.NM_ETA_INI_ESE_MAGGIORE
						)
					)
					AND (
						B.NM_ETA_INI_ESE_MINORE IS missing
						OR (

							d.eta < B.NM_ETA_INI_ESE_MINORE
						)
					)
			;QUIT;
%end;

/*** ESENZIONI - PS ******************************/

data TEMP.L1_ESE_WRK_&reg._1;
set TEMP.L1_ESE_WRK_&reg TEMP.L1_PS_ESE_WRK_&reg;
run;


			PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					A.ID_CLASSIFICAZIONE,
					A.ID_INDICATORE,
					A.ID_ANONIMO
				FROM
					TEMP.L1_ESE_WRK_&reg._1 A
					JOIN (select id_anonimo, sesso, eta from camp.anagrafica_4mln where cod_regione = "&reg.") D ON A.ID_ANONIMO=D.ID_ANONIMO
					LEFT JOIN input.CRIT_L2_ESE B ON A.ID_CLASSIFICAZIONE=B.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=B.ID_INDICATORE
				WHERE
					(
						B.NM_ETA_INI_ESE_MAGGIORE IS missing
						OR (
							d.eta  > B.NM_ETA_INI_ESE_MAGGIORE
						)
					)
					AND (
						B.NM_ETA_INI_ESE_MINORE IS missing
						OR (

							d.eta < B.NM_ETA_INI_ESE_MINORE
						)
					)
			;QUIT;


	/*** SDO MOD DIMISS ******************************/
/*%if %sysfunc(exist(TEMP.L1_SDO_MOD_DIMISS_WRK)) %then %do;
	PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					ID_CLASSIFICAZIONE,
					ID_INDICATORE,
					ID_ANONIMO
				FROM
					TEMP.L1_SDO_MOD_DIMISS_WRK_&reg A
					JOIN (select id_anonimo, sesso, eta from camp.anagrafica_4mln where cod_regione = "&reg.") D ON A.ID_ANONIMO=D.ID_ANONIMO
					LEFT JOIN input.CRIT_L2_DRG B ON A.ID_CLASSIFICAZIONE=B.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=B.ID_INDICATORE
				WHERE
					(
						B.NM_ETA_INI_ESE_MAGGIORE IS missing
						OR (
							d.eta  > B.NM_ETA_INI_ESE_MAGGIORE
						)
					)
					AND (
						B.NM_ETA_INI_ESE_MINORE IS missing
						OR (

							d.eta < B.NM_ETA_INI_ESE_MINORE
						)
					)
			;QUIT;
	;QUIT;
%end;*/

	/*** SDO DRG ******************************/

				PROC SQL;
				INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					A.ID_CLASSIFICAZIONE,
					A.ID_INDICATORE,
					A.ID_ANONIMO
				FROM
					TEMP.L1_SDO_DRG_WRK_&reg A
					JOIN (select id_anonimo, sesso, eta from camp.anagrafica_4mln where cod_regione = "&reg.") D ON A.ID_ANONIMO=D.ID_ANONIMO
					LEFT JOIN input.CRIT_L2_DRG B ON A.ID_CLASSIFICAZIONE=B.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=B.ID_INDICATORE
				WHERE
					(
						B.NM_ETA_MAGGIORE_DI IS missing
						OR (
							d.eta  > B.NM_ETA_MAGGIORE_DI
						)
					)
					AND (
						B.NM_ETA_MINORE_DI IS missing
						OR (

							d.eta < B.NM_ETA_MINORE_di
						)
					)
					AND (
						B.NM_RICOVERI_ALMENO IS missing
						OR (

							a.qta < B.NM_RICOVERI_ALMENO
						)
					)
				;QUIT;


/*** SDO DIAG - HOSPICE - PS ******************************/

data TEMP.L1_SDO_DIAG_WRK_&reg._1;
set TEMP.L1_SDO_DIAG_WRK_&reg TEMP.L1_HOSPICE_WRK_&reg TEMP.L1_PS_DIAG_WRK_&reg TEMP.L1_SISM_DIAG_WRK_&reg;
run;



			PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					A.ID_CLASSIFICAZIONE,
					A.ID_INDICATORE,
					A.ID_ANONIMO
				FROM
					TEMP.L1_SDO_DIAG_WRK_&reg._1 A
					JOIN (select id_anonimo, sesso, eta from camp.anagrafica_4mln where cod_regione = "&reg.") D ON A.ID_ANONIMO=D.ID_ANONIMO
					LEFT JOIN input.CRIT_L2_SDO_DIAG B ON A.ID_CLASSIFICAZIONE=B.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=B.ID_INDICATORE
				WHERE
					(
						B.NM_ETA_MAGGIORE_DI IS missing
						OR (
							d.eta  > B.NM_ETA_MAGGIORE_DI
						)
					)
					AND (
						B.NM_ETA_MINORE_DI IS missing
						OR (

							d.eta < B.NM_ETA_MINORE_di
						)
					)
					AND (
						B.NM_RICOVERI_ALMENO IS missing
						OR (

							a.qta < B.NM_RICOVERI_ALMENO
						)
					)
				;QUIT;



	/*** SDO DIAG ******************************/
/*
%if %sysfunc(exist(TEMP.L1_SDO_DIAG_WRK_&reg)) %then %do;
			PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					A.ID_CLASSIFICAZIONE,
					A.ID_INDICATORE,
					A.ID_ANONIMO
				FROM
					TEMP.L1_SDO_DIAG_WRK_&reg A
					JOIN (select id_anonimo, sesso, eta from camp.anagrafica_4mln where cod_regione = "&reg.") D ON A.ID_ANONIMO=D.ID_ANONIMO
					LEFT JOIN input.CRIT_L2_SDO_DIAG B ON A.ID_CLASSIFICAZIONE=B.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=B.ID_INDICATORE
				WHERE
					(
						B.NM_ETA_MAGGIORE_DI IS missing
						OR (
							d.eta  > B.NM_ETA_MAGGIORE_DI
						)
					)
					AND (
						B.NM_ETA_MINORE_DI IS missing
						OR (

							d.eta < B.NM_ETA_MINORE_di
						)
					)
					AND (
						B.NM_RICOVERI_ALMENO IS missing
						OR (

							a.qta < B.NM_RICOVERI_ALMENO
						)
					)
				;QUIT;
%end;


	/*** SDO INTERV ******************************/

	PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					ID_CLASSIFICAZIONE,
					ID_INDICATORE,
					ID_ANONIMO
				FROM
					TEMP.L1_SDO_INTERV_WRK_&reg
	;QUIT;


	/*** SDO ******************************/

	PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					ID_CLASSIFICAZIONE,
					ID_INDICATORE,
					ID_ANONIMO
				FROM
					TEMP.L1_SDO_WRK_&reg
		;QUIT;

	/*** FAR ******************************/

PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					A.ID_CLASSIFICAZIONE,
					A.ID_INDICATORE,
					A.ID_ANONIMO
				FROM
					temp.L1_FAR_WRK_&reg A
					JOIN (select id_anonimo, sesso, eta from camp.anagrafica_4mln where cod_regione = "&reg.") D ON A.ID_ANONIMO=D.ID_ANONIMO
					LEFT JOIN input.CRIT_L2_FAR B ON A.ID_CLASSIFICAZIONE=B.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=B.ID_INDICATORE
					LEFT JOIN input.PROFONDITA_TEMPORALE E ON A.ID_CLASSIFICAZIONE=E.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=E.ID_INDICATORE
				WHERE
					(
						B.NM_DDD_MAGGIORE IS MISSING
						OR (
							( A.GG_DDD / ( intnx("month",input(&DT_RIFERIMENTO.,ddmmyy8.), MIN(&PROFONDITA_FLUSSO,E.NM_PROFONDITA_DA_ANNI)*-12, 's')
								- ( intnx("month", input(&DT_RIFERIMENTO.,ddmmyy8.),MIN(&PROFONDITA_FLUSSO, E.NM_PROFONDITA_A_ANNI)*-12, 's') +1 ) ) )
								> ( B.NM_DDD_MAGGIORE / 100.00 )
						)
					)
					AND (
						B.NM_VAL_ALMENO IS MISSING
						OR (
							A.VAL >= NM_VAL_ALMENO
						)
					)
					AND (
						B.NM_EROGAZIONI_ALMENO IS MISSING
						OR (
							A.N_EROGAZ >= B.NM_EROGAZIONI_ALMENO
						)
					)
					AND (
						B.NM_ETA_MAGGIORE_DI IS MISSING
						OR (
							d.eta > B.NM_ETA_MAGGIORE_DI
						)
					)
					AND (
						B.NM_ETA_MINORE_DI IS MISSING
						OR (
							d.eta < B.NM_ETA_MINORE_DI
						)
					)
					AND (
						B.NM_QUANT_ALMENO IS MISSING
						OR (
							A.NM_QUANT_FARMACO >= B.NM_QUANT_ALMENO
						)
					)
					AND (
						B.MIN_GG_TER IS MISSING
						OR (
							A.GG_DDD >= B.MIN_GG_TER
						)
					)
	;QUIT;


	/*** AMB ******************************/
/*
%if %sysfunc(exist(TEMP.L1_AMB_WRK_&reg)) %then %do;
PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					A.ID_CLASSIFICAZIONE,
					A.ID_INDICATORE,
					A.ID_ANONIMO
				FROM
					TEMP.L1_AMB_WRK_&reg A
					LEFT JOIN input.CRIT_L2_AMB B ON A.ID_CLASSIFICAZIONE=B.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=B.ID_INDICATORE
				WHERE
					(
						B.NM_PRESTAZIONI_ALMENO IS NULL
						OR (
							( A.QTA / ( ( ( input(&DT_RIFERIMENTO.,ddmmyy8.) + 1 ) - A.DT_INI_RIF ) / 365.00 ) ) >= B.NM_PRESTAZIONI_ALMENO
						)
						)
						AND (
						B.NM_PRESTAZIONI_ALMENO2 IS NULL
						OR (
							A.QTA  >= B.NM_PRESTAZIONI_ALMENO2
					)
					)
;QUIT;
%END;

/*** AMB - PS******************************/

data TEMP.L1_AMB_WRK_&reg._1;
set TEMP.L1_AMB_WRK_&reg TEMP.L1_PS_AMB_WRK_&reg;
run;


PROC SQL;
			INSERT INTO TEMP.L2_WRK_&reg (
					ID_CLASSIFICAZIONE
					, ID_INDICATORE
					, ID_ANONIMO
				)
				SELECT
					A.ID_CLASSIFICAZIONE,
					A.ID_INDICATORE,
					A.ID_ANONIMO
				FROM
					TEMP.L1_AMB_WRK_&reg._1 A
					LEFT JOIN input.CRIT_L2_AMB B ON A.ID_CLASSIFICAZIONE=B.ID_CLASSIFICAZIONE AND A.ID_INDICATORE=B.ID_INDICATORE
				WHERE
					(
						B.NM_PRESTAZIONI_ALMENO IS NULL
						OR (
							( A.QTA / ( ( ( input(&DT_RIFERIMENTO.,ddmmyy8.) + 1 ) - A.DT_INI_RIF ) / 365.00 ) ) >= B.NM_PRESTAZIONI_ALMENO
						)
						)
						AND (
						B.NM_PRESTAZIONI_ALMENO2 IS NULL
						OR (
							A.QTA  >= B.NM_PRESTAZIONI_ALMENO2
					)
					)
;QUIT;


%MEND;

%L2(ID_CLASSIFICAZIONE=9,reg=010);
%L2(ID_CLASSIFICAZIONE=9,reg=020);
%L2(ID_CLASSIFICAZIONE=9,reg=030);
%L2(ID_CLASSIFICAZIONE=9,reg=041);
%L2(ID_CLASSIFICAZIONE=9,reg=042);
%L2(ID_CLASSIFICAZIONE=9,reg=050);
%L2(ID_CLASSIFICAZIONE=9,reg=060);
%L2(ID_CLASSIFICAZIONE=9,reg=070);
%L2(ID_CLASSIFICAZIONE=9,reg=080);
%L2(ID_CLASSIFICAZIONE=9,reg=090);
%L2(ID_CLASSIFICAZIONE=9,reg=100);
%L2(ID_CLASSIFICAZIONE=9,reg=110);
%L2(ID_CLASSIFICAZIONE=9,reg=120);
%L2(ID_CLASSIFICAZIONE=9,reg=130);
%L2(ID_CLASSIFICAZIONE=9,reg=140);
%L2(ID_CLASSIFICAZIONE=9,reg=150);
%L2(ID_CLASSIFICAZIONE=9,reg=160);
%L2(ID_CLASSIFICAZIONE=9,reg=170);
%L2(ID_CLASSIFICAZIONE=9,reg=180);
%L2(ID_CLASSIFICAZIONE=9,reg=190);
%L2(ID_CLASSIFICAZIONE=9,reg=200);
