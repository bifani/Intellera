proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\ANALISI_CRONICI_NUOVO.txt" new;
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


%macro ANALISI_CRONICI(ID_CLASSIFICAZIONE=,reg=);


*ESTRAZIONE ID CRONICI E PATOLOGIE ASSOCIATE;
	PROC SQL;
		CREATE TABLE temp.ID_PAT_CRONICI_&reg AS
		SELECT DISTINCT
			A.ID_CLASSIFICAZIONE,
			A.ID_ANONIMO,
			B.ID_PATOLOGIA
		from temp.FINALE_L4_&reg AS A LEFT JOIN (select * FROM input.dominio_indicatori where id_classificazione = &id_classificazione) AS B
			ON A.ID_INDICATORE=B.ID_INDICATORE
		;QUIT;

*COSTO PER I CRONICI;

	*elenco cronici;
	PROC SQL;
		create table temp.lista_cronici_&reg as select
			A.id_classificazione,
			A.id_anonimo,
			B.ETA,
			B.SESSO
		from (SELECT DISTINCT ID_CLASSIFICAZIONE, ID_ANONIMO FROM temp.ID_PAT_CRONICI_&reg) A
		LEFT JOIN (select id_anonimo, sesso, eta from camp.anagrafica_4mln where cod_regione = "&reg.") b
		on a.id_anonimo = b.id_anonimo;
	QUIT;

	*costo farma;
	PROC SQL;
		CREATE TABLE temp.COSTO_FARMA_&reg as select
			a.id_classificazione,
			a.id_anonimo,
			sum(COALESCE(b.costo_servizio,0)) as costo_farma
		from temp.lista_cronici_&reg a
		left join (select id_anonimo, costo_servizio from output.farma_&reg where anno=2019) B
		on a.id_anonimo = b.id_anonimo
		GROUP BY A.ID_CLASSIFICAZIONE, A.ID_ANONIMO;
	QUIT;

	*costo amb;
	PROC SQL;
		CREATE TABLE temp.COSTO_amb_&reg as select
			a.id_classificazione,
			a.id_anonimo,
			sum(COALESCE(b.tariffa_prest, 0)) as costo_amb
		from temp.lista_cronici_&reg a
		left join (select id_anonimo, tariffa_prest from output.amb_&reg where anno=2019) B
		on a.id_anonimo = b.id_anonimo
		GROUP BY A.ID_CLASSIFICAZIONE, A.ID_ANONIMO;
	QUIT;

	*costo sdo ;
		PROC SQL;
		CREATE TABLE temp.COSTO_SDO_&reg as select
			a.id_classificazione,
			a.id_anonimo,
			sum(COALESCE(b.AM_VALORE_TOT_EURO, 0)) as costo_sdo
		from temp.lista_cronici_&reg a
		left join (select id_anonimo, AM_VALORE_TOT_EURO from output.SDO_&reg where anno=2019) B
		on a.id_anonimo = b.id_anonimo
		GROUP BY A.ID_CLASSIFICAZIONE, A.ID_ANONIMO;
	QUIT;

	*costo Hospice;

		PROC SQL;
		CREATE TABLE temp.COSTO_HOSPICE_&reg as select
			a.id_classificazione,
			a.id_anonimo,
			sum(COALESCE(b.VAL_HOSPICE, 0)) as costo_hospice
		from temp.lista_cronici_&reg a
		left join (select id_anonimo, VAL_HOSPICE from output.hospice_&reg where anno=2019) B
		on a.id_anonimo = b.id_anonimo
		GROUP BY A.ID_CLASSIFICAZIONE, A.ID_ANONIMO;
	QUIT;

	*costo Farma Territoriale;

	PROC SQL;
	CREATE TABLE temp.COSTO_FARMA_TERR_&reg as select
		a.id_classificazione,
		a.id_anonimo,
		sum(COALESCE(b.QUOTA_SSR, 0)) as costo_farma_terr /*da valutare VAL_FAR_TERR o QUOTA_SSR*/
	from temp.lista_cronici_&reg a
	left join (select id_anonimo, QUOTA_SSR from output.far_territoriale_&reg where anno=2019) B
	on a.id_anonimo = b.id_anonimo
	GROUP BY A.ID_CLASSIFICAZIONE, A.ID_ANONIMO;
	QUIT;

	*costo EMUR (Pronto Soccorso / PS);

	PROC SQL;
	CREATE TABLE temp.COSTO_PS_&reg as select
		a.id_classificazione,
		a.id_anonimo,
		sum(COALESCE(b.importo_lordo, 0)) as costo_ps
	from temp.lista_cronici_&reg a
	left join (select id_anonimo, importo_lordo from output.ps_&reg where anno=2019) B
	on a.id_anonimo = b.id_anonimo
	GROUP BY A.ID_CLASSIFICAZIONE, A.ID_ANONIMO;
	QUIT;


	*COSTO TOTALE;
	PROC SQL;
	CREATE TABLE temp.COSTO_TOT_&reg AS SELECT
		A.id_classificazione,
		a.id_anonimo,
		A.ETA,
		A.SESSO,
		COALESCE(B.COSTO_FARMA, 0) AS COSTO_FARMA,
		COALESCE(C.COSTO_AMB, 0) AS COSTO_AMB,
		COALESCE(D.COSTO_SDO, 0) AS COSTO_SDO,
		COALESCE(E.COSTO_HOSPICE, 0) AS COSTO_HOSPICE,
		COALESCE(F.COSTO_FARMA_TERR, 0) AS COSTO_FARMA_TERR,
		COALESCE(G.COSTO_PS, 0) AS COSTO_PS,
		SUM(COALESCE(C.COSTO_AMB,0), COALESCE(D.COSTO_SDO,0), COALESCE(B.COSTO_FARMA,0),COALESCE(E.COSTO_HOSPICE,0)
,COALESCE(F.costo_farma_terr,0),COALESCE(G.costo_ps,0)) AS VAL_TOT
	FROM temp.lista_cronici_&reg a
	LEFT JOIN temp.COSTO_FARMA_&reg B
		ON A.ID_ANONIMO = B.ID_ANONIMO
	LEFT JOIN temp.COSTO_AMB_&reg C
		ON A.ID_ANONIMO = C.ID_ANONIMO
	LEFT JOIN temp.COSTO_SDO_&reg D
		ON A.ID_ANONIMO = D.ID_ANONIMO
	LEFT JOIN temp.COSTO_HOSPICE_&reg E
		ON A.ID_ANONIMO = E.ID_ANONIMO
	LEFT JOIN temp.COSTO_FARMA_TERR_&reg F
		ON A.ID_ANONIMO = F.ID_ANONIMO
	LEFT JOIN temp.COSTO_PS_&reg G
	ON A.ID_ANONIMO = G.ID_ANONIMO
	;QUIT;

    *COSTO PER ETA CRONICI;
	PROC SQL;
		CREATE TABLE TEMP.COSTOXETA_CRONICI_&reg AS
		SELECT
			ETA,
			SESSO,
			COUNT(ID_ANONIMO) AS FREQ,
			MEAN(COALESCE(COSTO_AMB,0)) AS MEAN_VAL_AMB,
			MEAN(COALESCE(COSTO_SDO,0)) AS MEAN_VAL_RICOVERI,
			MEAN(COALESCE(COSTO_FARMA,0)) AS MEAN_VAL_FARMA,
			MEAN(COALESCE(COSTO_HOSPICE,0)) AS MEAN_VAL_HOSPICE,
			MEAN(COALESCE(COSTO_FARMA_TERR,0)) AS MEAN_VAL_FARMA_TERR,
			MEAN(COALESCE(COSTO_PS,0)) AS MEAN_VAL_PS,
			MEAN(VAL_TOT) AS MEAN_VAL
		FROM TEMP.COSTO_TOT_&reg
		GROUP BY ETA, SESSO
		ORDER BY ETA, SESSO
	;QUIT;

	DATA TEMP.COSTOXETA_CRONICI_&reg._FILT;
	SET TEMP.COSTOXETA_CRONICI_&reg;
	WHERE FREQ GE 3;
	REGIONE="&&reg.";
	RUN;
%MEND;


%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=010);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=020);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=030);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=041);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=042);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=050);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=060);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=070);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=080);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=090);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=100);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=110);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=120);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=130);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=140);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=150);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=160);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=170);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=180);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=190);
%ANALISI_CRONICI(ID_CLASSIFICAZIONE=9,reg=200);



/* ESPORTO I DATI AGGREGATI IN EXCEL */


data COSTOXETA_CRONICI_TOT_FILT;
set
TEMP.COSTOXETA_CRONICI_010_FILT TEMP.COSTOXETA_CRONICI_020_FILT TEMP.COSTOXETA_CRONICI_030_FILT
TEMP.COSTOXETA_CRONICI_041_FILT TEMP.COSTOXETA_CRONICI_042_FILT
TEMP.COSTOXETA_CRONICI_050_FILT TEMP.COSTOXETA_CRONICI_060_FILT
TEMP.COSTOXETA_CRONICI_070_FILT TEMP.COSTOXETA_CRONICI_080_FILT TEMP.COSTOXETA_CRONICI_090_FILT
TEMP.COSTOXETA_CRONICI_100_FILT
TEMP.COSTOXETA_CRONICI_110_FILT TEMP.COSTOXETA_CRONICI_120_FILT TEMP.COSTOXETA_CRONICI_130_FILT
TEMP.COSTOXETA_CRONICI_140_FILT TEMP.COSTOXETA_CRONICI_150_FILT TEMP.COSTOXETA_CRONICI_160_FILT
TEMP.COSTOXETA_CRONICI_170_FILT TEMP.COSTOXETA_CRONICI_180_FILT TEMP.COSTOXETA_CRONICI_190_FILT
TEMP.COSTOXETA_CRONICI_200_FILT;
run;


proc export data = COSTOXETA_CRONICI_TOT_FILT
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\COSTO_X_ETA_CRONICI_TOT_FILT.xlsx"
	dbms = xlsx
	replace;
run;

/*
%macro risultati(reg=);

proc export data = TEMP.COSTOXETA_CRONICI_&reg._FILT
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\COSTO_X_ETA_CRONICI_&reg._FILT.xlsx"
	dbms = xlsx
	replace;
run;

proc export data = temp.COSTO_TOT_&reg
	outfile = "T:\202204_modello\Output camp\tabelle_analisi_codice\ANALISI\COSTO_TOT_&reg._NOFILT.xlsx"
	dbms = xlsx
	replace;
run;

%mend;

%risultati(reg=010);
%risultati(reg=020);
%risultati(reg=030);
%risultati(reg=041);
%risultati(reg=042);
%risultati(reg=050);
%risultati(reg=060);
%risultati(reg=070);
%risultati(reg=080);
%risultati(reg=090);
%risultati(reg=100);
%risultati(reg=110);
%risultati(reg=120);
%risultati(reg=130);
%risultati(reg=140);
%risultati(reg=150);
%risultati(reg=160);
%risultati(reg=170);
%risultati(reg=180);
%risultati(reg=190);
%risultati(reg=200);
*/
