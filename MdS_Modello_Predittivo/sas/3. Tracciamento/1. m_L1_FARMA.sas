OPTIONS MPRINT;
OPTIONS COMPRESS =YES;
proc printto log="C:\Users\r.blaco\Desktop\scripts Modello predittivo 20220706\logs\L1_FARMACEUTICA.txt" new;
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




%macro L1_FAR(ID_CLASSIFICAZIONE=, reg=); /* LIVELLO 1 - FARMACEUTICA versione  */

		PROC SQL;
		SELECT count(*) INTO :check_indicatore FROM input.CRIT_L1_FAR WHERE ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.
		;QUIT;

	/**********PROFONDITA TEMP********************/
	%IF %eval(&check_indicatore. > 0) %THEN %DO;
		PROC SQL;
			create table &temp..CRIT_L1_FAR_PROFONDITA
			AS SELECT A.*,
				LENGTH(ID_ATC) AS LEN_ATC,
				NM_PROFONDITA_DA_ANNI,
				NM_PROFONDITA_A_ANNI
			FROM input.CRIT_L1_FAR A
			LEFT JOIN
			input.PROFONDITA_TEMPORALE B
			ON A.ID_CLASSIFICAZIONE = B.ID_CLASSIFICAZIONE
				AND A.ID_INDICATORE = B.ID_INDICATORE
			where a.id_classificazione = &id_classificazione.
		;QUIT;

	/**********DDD LAST******************* ----------> Questa la cancelliamo perch� la abbiamo fatta girare offline*/
	/*	PROC SQL;
		CREATE TABLE &temp..DDD_LAST
			AS SELECT
				COD_AIC,
				G_TER FROM &STAGING..DOMINIO_DDD_AIC
			group by COD_AIC
			having coalesce(DFV, '12DEC2099'd) = max(coalesce(DFV, '12DEC2099'd))
			order by cod_aic
		;QUIT;*/

	/* aggancio l'atc alla tabella di farmaceutica della regione */
	/* VERSIONE 1
		proc sql;
		create table temp.far_&reg._atc as select a.*, b.ATC as  id_atc
			from output.farma_&reg a left join input.ddd_last b
			on a.cod_aic = b.cod_aic;
	quit; */
	/* VERSIONE OTTIMIZZATA*/


		/*DA FARE ATTENZIONE!*/

		data temp.farma_&reg._a temp.farma_&reg._b;
		set output.farma_&reg(KEEP = anno cod_ricetta data_erogazione num_farma_ric cod_aic quantita costo_servizio costo_acquisto id_anonimo);
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

	/*	proc sort data = input.ddd_last;
			by cod_aic;
		run; */

	/*filtro soltanto gli atc presenti dentro ai criteri traccianti*/

	proc sql;
		create table temp.atc_list as select
			distinct id_atc
			from input.crit_l1_far
			where id_classificazione = &id_classificazione;
	quit;

	proc sql;
		create table temp.far_&reg._filt as select *
			from temp.far_&reg._atc
			where id_atc <> "" /*in (select id_atc from temp.atc_list)*/;
	quit;

	proc delete data=temp.atc_list;
	run;

	/**********FARMACEUTICA********************/
			proc sql;
			create table &temp..L1_FAR_WRK_01_&reg as
				select
				ID_CLASSIFICAZIONE,
				ID_INDICATORE,
				NM_PROFONDITA_DA_ANNI,
				NM_PROFONDITA_A_ANNI,
				costo_servizio,
				/*AM_VALORE_TICKET,*/
				data_erogazione,
				cod_aic,
				ID_ANONIMO,
				FAR.ID_ATC,
				cod_ricetta,
				/*NM_FATTORE_CONV,*/
				quantita
				from temp.far_&reg._filt AS FAR,
					 &temp..CRIT_L1_FAR_PROFONDITA AS T
				WHERE ID_CLASSIFICAZIONE = &ID_CLASSIFICAZIONE.
					AND(
						(T.FL_INI='0' AND compress(FAR.ID_ATC, ".")=T.ID_ATC) OR
						(T.FL_INI='1' AND substr(compress(FAR.ID_ATC,"."),1,T.LEN_ATC)=T.ID_ATC)
						)
				order by cod_aic
		;quit;

			proc sql;
			CREATE TABLE &temp..L1_FAR_WRK_02_&reg
				AS SELECT
					A.ID_CLASSIFICAZIONE,
					A.ID_INDICATORE,
					NM_PROFONDITA_DA_ANNI,
					NM_PROFONDITA_A_ANNI,
					a.costo_servizio,
					/*a.AM_VALORE_TICKET,*/
					a.data_erogazione,
					a.cod_aic,
					a.ID_ANONIMO,
					a.ID_ATC,
					a.cod_ricetta,
					/*a.NM_FATTORE_CONV,*/
					a.quantita,
					b.G_TER	FROM &temp..L1_FAR_WRK_01_&reg  A
					LEFT JOIN input.DDD_LAST B
				ON B.COD_AIC = A.cod_aic
			;quit;


		PROC SQL;
			CREATE TABLE &temp..L1_FAR_WRK_03_&reg AS
				SELECT
					ID_CLASSIFICAZIONE,
					ID_INDICATORE,
					ID_ANONIMO,
					quantita,
					(coalesce(G_TER, 1)*coalesce(quantita, 1)) as G_TER,
					data_erogazione AS DT_INI_RIF,
					costo_servizio AS VAL,
					cod_ricetta
						FROM
							&temp..L1_FAR_WRK_02_&reg
						WHERE
							data_erogazione
									BETWEEN intnx("month",input(&DT_RIFERIMENTO., ddmmyy8.), NM_PROFONDITA_A_ANNI*-12,'s')+1
									AND intnx("month",input(&DT_RIFERIMENTO.,ddmmyy8.), NM_PROFONDITA_DA_ANNI*-12, 's');
	QUIT;




	/**********UNISCE********************/
		PROC SQL;
			create table &temp..L1_FAR_WRK_&reg as
			SELECT
				ID_CLASSIFICAZIONE,
				ID_INDICATORE,
				ID_ANONIMO,
				SUM(quantita) as NM_QUANT_FARMACO,
				SUM(G_TER) AS GG_DDD,
				MIN(DT_INI_RIF) AS DT_INI_RIF FORMAT DDMMYY10.,
				MAX(DT_INI_RIF) AS DT_FIN_RIF FORMAT DDMMYY10.,
				SUM(VAL) AS VAL,
				count(distinct cod_ricetta) as N_EROGAZ
				FROM &temp..L1_FAR_WRK_03_&reg
                GROUP BY
							ID_CLASSIFICAZIONE,
							ID_INDICATORE,
							ID_ANONIMO
		;QUIT;


	%END;
	proc delete data=&temp..L1_FAR_WRK_03_&reg &temp..L1_FAR_WRK_02_&reg &temp..L1_FAR_WRK_01_&reg
					temp.farma_&reg._a temp.farma_&reg._b temp.farma_&reg._sort temp.far_&reg._atc temp.ddd_last_sort;
		run;
%MEND;


%L1_FAR(ID_CLASSIFICAZIONE=9,reg=010);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=020);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=030);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=041);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=042);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=050);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=060);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=070);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=080);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=090);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=100);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=110);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=120);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=130);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=140);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=150);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=160);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=170);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=180);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=190);
%L1_FAR(ID_CLASSIFICAZIONE=9,reg=200);
