										/*DIALISI*/

/**********************************************************************************************************/
/********************************************AMB***********************************************************/
/**********************************************************************************************************/

/*PERSONE CHE FANNO DIALISI*/
/*L1*/

proc sql;
create table l1_amb_NF02 as select distinct ID_ANONIMO ,ID_INDICATORE
from temp.l1_amb_wrk_030
where substr(ID_INDICATORE,1,4) in ("NF02")
;quit;

PROC SQL;/*798*/
select COUNT(distinct ID_anonimo) as persone
from l1_amb_NF02
;QUIT;

/*DATI DI INPUT*/


		PROC SQL;
		CREATE TABLE DIALISI as select ID_ANONIMO,NUM_PRESTAZIONI, compress(ID_PRESTAZIONE,".") as PRESTAZIONE
		from output.AMB_030
		WHERE ANNO=2019
		;QUIT;

		data DIALISI1;
		set DIALISI;
		where substr(PRESTAZIONE,1,4) IN ("3995","5498");
		run;

		PROC SQL;/*798*/
		CREATE TABLE DIALISI2_AMB AS SELECT DISTINCT id_anonimo, SUM(NUM_PRESTAZIONI) as numero_prest
		FROM DIALISI1
		GROUP BY ID_ANONIMO
		;QUIT;

		/*PRESTAZIONI >= 50*/
		data DIALISI3_AMB;
		set DIALISI2;
		where numero_prest >=50;
		run;


/**********************************************************************************************************/
/********************************************SDO_INTERVENTI************************************************/
/**********************************************************************************************************/
/*L1*/

proc sql;
create table l1_sdo_int_NF02 as select distinct ID_ANONIMO ,ID_INDICATORE
from temp.l1_sdo_interv_wrk_030
where substr(ID_INDICATORE,1,4) in ("NF02")
;quit;

PROC SQL;/*1.175*/
select COUNT(distinct ID_anonimo) as persone
from l1_sdo_int_NF02
;QUIT;

/*L2*/

proc sql;
create table l2_sdo_int_NF02 as select distinct ID_ANONIMO ,ID_INDICATORE
from temp.l2_wrk_030
where substr(ID_INDICATORE,1,4) in ("NF02")
;quit;

PROC SQL;/*1.175*/
select COUNT(distinct ID_anonimo) as persone
from l1_sdo_int_NF02
;QUIT;

proc freq data=input.crit_l1_amb;table id_classificazione;run;

/**********************************************************************************************************/
/********************************************SDO_DIA*******************************************************/
/**********************************************************************************************************/
/*L1*/

proc sql;
create table l1_sdo_dia_NF02 as select distinct ID_ANONIMO ,ID_INDICATORE
from temp.l1_sdo_diag_wrk_030
where substr(ID_INDICATORE,1,4) in ("NF02")
;quit;

PROC SQL;/*1.339*/
select COUNT(distinct ID_anonimo) as persone
from l1_sdo_dia_NF02
;QUIT;

/*DATI DI INPUT*/
		PROC SQL;
		CREATE TABLE DIALISI_SDO_INTV as select ID_ANONIMO,id_interv_princ,id_interv_sec_1,id_interv_sec_2,id_interv_sec_3,
		id_interv_sec_4,id_interv_sec_5
		from output.SDO_030
		WHERE ANNO in (2014,2015,2016,2017,2018,2019)
		;QUIT;

		data DIALISI_SDO_INTV1;
		set DIALISI_SDO_INTV;
		where
		compress(ID_INTERV_PRINC,".")="3895" OR
		compress(ID_INTERV_SEC_1,".")="3895" OR
		compress(ID_INTERV_SEC_2,".")="3895" OR
		compress(ID_INTERV_SEC_3,".")="3895" OR
		compress(ID_INTERV_SEC_4,".")="3895" OR
		compress(ID_INTERV_SEC_5,".")="3895" OR

		compress(ID_INTERV_PRINC,".")="3995" OR
		compress(ID_INTERV_SEC_1,".")="3995" OR
		compress(ID_INTERV_SEC_2,".")="3995" OR
		compress(ID_INTERV_SEC_3,".")="3995" OR
		compress(ID_INTERV_SEC_4,".")="3995" OR
		compress(ID_INTERV_SEC_5,".")="3995" OR

		compress(ID_INTERV_PRINC,".")="5498" OR
		compress(ID_INTERV_SEC_1,".")="5498" OR
		compress(ID_INTERV_SEC_2,".")="5498" OR
		compress(ID_INTERV_SEC_3,".")="5498" OR
		compress(ID_INTERV_SEC_4,".")="5498" OR
		compress(ID_INTERV_SEC_5,".")="5498"
        ;run;

		PROC SQL;
		CREATE TABLE DIALISI_SDO_INTV2 AS SELECT DISTINCT ID_ANONIMO
		FROM DIALISI_SDO_INTV1
		;QUIT;

		PROC SQL;/*1.093 + 186= 1279*/
		SELECT count(distinct ID_ANONIMO)
		FROM DIALISI_SDO_INTV2
		;QUIT;


		/*****/

/*DATI DI INPUT*/
		PROC SQL;
		CREATE TABLE DIALISI_SDO_INTV as select ID_ANONIMO,id_interv_princ,id_interv_sec_1,id_interv_sec_2,id_interv_sec_3,
		id_interv_sec_4,id_interv_sec_5
		from output.SDO_030
		WHERE ANNO in (2014,2015,2016,2017,2018,2019)
		;QUIT;

		data DIALISI_SDO_INTV1;
		set DIALISI_SDO_INTV;
		where
		compress(ID_INTERV_PRINC,".")="3895" OR
		compress(ID_INTERV_SEC_1,".")="3895" OR
		compress(ID_INTERV_SEC_2,".")="3895" OR
		compress(ID_INTERV_SEC_3,".")="3895" OR
		compress(ID_INTERV_SEC_4,".")="3895" OR
		compress(ID_INTERV_SEC_5,".")="3895" OR

		compress(ID_INTERV_PRINC,".")="3995" OR
		compress(ID_INTERV_SEC_1,".")="3995" OR
		compress(ID_INTERV_SEC_2,".")="3995" OR
		compress(ID_INTERV_SEC_3,".")="3995" OR
		compress(ID_INTERV_SEC_4,".")="3995" OR
		compress(ID_INTERV_SEC_5,".")="3995" OR

		compress(ID_INTERV_PRINC,".")="5498" OR
		compress(ID_INTERV_SEC_1,".")="5498" OR
		compress(ID_INTERV_SEC_2,".")="5498" OR
		compress(ID_INTERV_SEC_3,".")="5498" OR
		compress(ID_INTERV_SEC_4,".")="5498" OR
		compress(ID_INTERV_SEC_5,".")="5498"
        ;run;

		PROC SQL;
		CREATE TABLE DIALISI_SDO_INTV2 AS SELECT DISTINCT ID_ANONIMO
		FROM DIALISI_SDO_INTV1
		;QUIT;

		PROC SQL;/*1.093*/
		SELECT count(distinct ID_ANONIMO)
		FROM DIALISI_SDO_INTV2
		;QUIT;

		/*************************************************************************************************/

		/************************************* DIAGNOSI SDO ****************************************************/

		/*************************************************************************************************/
		/*DATI DI INPUT*/




		PROC SQL;
		CREATE TABLE DIALISI_SDO as select ID_ANONIMO,id_diag_prim,
		id_diag_sec_1, id_diag_sec_2,id_diag_sec_3,
		id_diag_sec_4,id_diag_sec_5
		from output.SDO_030
		WHERE ANNO in (2014,2015,2016,2017,2018,2019)
		;QUIT;

		data aaaa;
		set DIALISI_SDO;
		where id_diag_prim like "V%";
		run;



		data DIALISI_SDO1;
		set DIALISI_SDO;
		where
		compress(id_diag_prim,".")= "V5631" OR
		compress(id_diag_sec_1,".")="V5631" OR
		compress(id_diag_sec_2,".")="V5631" OR
		compress(id_diag_sec_3,".")="V5631" OR
		compress(id_diag_sec_4,".")="V5631" OR
		compress(id_diag_sec_5,".")="V5631" OR

		compress(id_diag_prim,".") ="V5632" OR
		compress(id_diag_sec_1,".")="V5632" OR
		compress(id_diag_sec_2,".")="V5632" OR
		compress(id_diag_sec_3,".")="V5632" OR
		compress(id_diag_sec_4,".")="V5632" OR
		compress(id_diag_sec_5,".")="V5632"
        ;run;

		PROC SQL;
		CREATE TABLE DIALISI_SDO2 AS SELECT DISTINCT ID_ANONIMO
		FROM DIALISI_SDO1
		;QUIT;

		PROC SQL;/*186*/
		SELECT count(distinct ID_ANONIMO)
		FROM DIALISI_SDO2
		;QUIT;
