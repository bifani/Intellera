proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\L1_AMBULATORIALE.txt" new;
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


/* LIVELLO 1 - AMBULATORIALE*/
/* LIVELLO 1 - AMBULATORIALE*/

%macro L1_AMB(ID_CLASSIFICAZIONE=,reg=);

proc sql;
	SELECT count(*) INTO :check_indicatore FROM INPUT.CRIT_L1_AMB WHERE ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.;
quit;


	%IF %eval(&check_indicatore. > 0) %THEN %DO;

			proc sql;
			create table temp.prestazioni_&reg as select *
			from INPUT.CRIT_L1_AMB
			where ID_CLASSIFICAZIONE=&ID_CLASSIFICAZIONE and ID_PRESTAZIONE <> "" and cod_reg in ("&reg.")
			;quit;


			proc sql;
			create table temp.L1_AMB_WRK_01_&reg as
				select
						CRIT.ID_CLASSIFICAZIONE,
						CRIT.ID_INDICATORE,
						amb.anno,
						AMB.ID_ANONIMO,
						AMB.NM_QUANTITA,
						COALESCE(AMB.dt_erogazione,MDY(7,1,ANNO)) AS DT_EROGAZIONE,
						(AMB.NUM_PRESTAZIONI*AMB.TARIFFA_PREST) as AM_VALORE_LORDO,
						/*non abbiamo il "valore lordo" (AM_VALORE_LORDO) ma la variabile TOT_RICETTA che rappresenta
						Valore totale delle prestazioni come somma dei singoli importi derivati dal nomenclatore.*/
						amb.cod_branca
				from output.AMB_&reg AMB,
					temp.prestazioni_&reg CRIT
				WHERE
					ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.

					AND(
						(CRIT.FL_INI='0' AND compress(AMB.ID_PRESTAZIONE, ".")=CRIT.ID_PRESTAZIONE) OR
						(CRIT.FL_INI='1' AND substr(compress(AMB.ID_PRESTAZIONE,"."),1,length(CRIT.ID_PRESTAZIONE))=CRIT.ID_PRESTAZIONE)
						)
			;quit;

		PROC SQL;
			CREATE TABLE TEMP.L1_AMB_WRK_&reg AS
				SELECT
					ID_CLASSIFICAZIONE,
					ID_INDICATORE,
					ID_ANONIMO,
					SUM(QTA) AS QTA,
					SUM(VAL) AS VAL,
					MIN(DT_INI_RIF) AS DT_INI_RIF FORMAT DDMMYY10.,
					MAX(DT_INI_RIF) AS DT_FIN_RIF FORMAT DDMMYY10.
				FROM (
					SELECT
							AMB_F.ID_CLASSIFICAZIONE,
							AMB_F.ID_INDICATORE,
							AMB_F.ID_ANONIMO,
							AMB_F.NM_QUANTITA AS QTA,
							AMB_F.AM_VALORE_LORDO AS VAL,
							AMB_F.COD_BRANCA,
							AMB_F.DT_EROGAZIONE AS DT_INI_RIF
						FROM
							INPUT.PROFONDITA_TEMPORALE C,
							temp.L1_AMB_WRK_01_&reg AMB_F
						where
							AMB_F.ID_CLASSIFICAZIONE=C.ID_CLASSIFICAZIONE
							AND AMB_F.ID_INDICATORE=C.ID_INDICATORE
							AND ((AMB_F.DT_EROGAZIONE BETWEEN intnx("month",input(&DT_RIFERIMENTO., ddmmyy8.), C.NM_PROFONDITA_A_ANNI*-12,'s')+1
															AND intnx("month",input(&DT_RIFERIMENTO.,ddmmyy8.), C.NM_PROFONDITA_DA_ANNI*-12,'s'))
								OR (mdy(07,01,amb_F.anno) between intnx("month",input(&DT_RIFERIMENTO., ddmmyy8.), C.NM_PROFONDITA_A_ANNI*-12,'s')+1
															AND intnx("month",input(&DT_RIFERIMENTO.,ddmmyy8.), C.NM_PROFONDITA_DA_ANNI*-12,'s')))
				) AA
				GROUP BY
					ID_CLASSIFICAZIONE,
					ID_INDICATORE,
					ID_ANONIMO
				;quit;

				/*"month",input(&DT_RIFERIMENTO., ddmmyy8.) --> mdy(1,1,AMB_F.anno)*/

		proc delete data = temp.L1_AMB_WRK_01_&reg temp.prestazioni_&reg;
		run;

	%END;
%MEND;


/* prova macro
*/
/*
%let dt_riferimento = "31122018";
%L1_AMB(ID_CLASSIFICAZIONE = 9);
*/

%L1_AMB(ID_CLASSIFICAZIONE=9,reg=010);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=020);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=030);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=041);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=042);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=050);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=060);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=070);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=080);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=090);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=100);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=110);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=120);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=130);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=140);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=150);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=160);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=170);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=180);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=190);
%L1_AMB(ID_CLASSIFICAZIONE=9,reg=200);
