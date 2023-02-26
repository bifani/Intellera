%macro stratification_analysis();

	proc sql;
		create table info_mono_str as
		select STR_GROUP,
			sum(FREQ) as FREQ_MONO,
			sum(SUM_VAL) as COSTO_TOTALE_MONO,
			sum(SUM_VAL)/ sum(FREQ) as COSTO_MEDIO_MONO,
			sum(SUM_SQUARE_VAL)/sum(FREQ)-(sum(SUM_VAL)/sum(FREQ))**2 as VARIANCE_MONO,
			sqrt(sum(SUM_SQUARE_VAL)/sum(FREQ)-(sum(SUM_VAL)/sum(FREQ))**2) as SD_MONO,
			sum(SUM_VAL_AMB) as COSTO_TOTALE_AMB_MONO,
			sum(SUM_RICOVERI) as COSTO_TOTALE_RICOVERI_MONO,
			sum(SUM_COSTO_FARMA) as COSTO_TOTALE_FARMA_MONO,
			sum(SUM_FARMA_TERR) as COSTO_TOTALE_FARMA_TERR_MONO,
			sum(SUM_HOSPICE) as COSTO_TOTALE_HOSPICE_MONO,
			sum(SUM_PS) as COSTO_TOTALE_PS_MONO


		from output.comb_str
		where /*N_PAT_INI=1*/N_PAT=1
		group by STR_GROUP;
	quit;				

	proc sql;
		create table output.analysis_stratification as
		select B.STR_GROUP,
			/*D.DESC_STR_GROUP, */
			B.STR_RANKING,
			A.*,
			C.*
		from
			(select DISTINCT STR_GROUP,
				sum(FREQ) as FREQ,
				sum(SUM_VAL) as COSTO_TOTALE,
				sum(SUM_VAL)/sum(FREQ) as COSTO_MEDIO,
				sum(SUM_SQUARE_VAL)/sum(FREQ)-(sum(SUM_VAL)/sum(FREQ))**2 as VARIANCE,
				sqrt(sum(SUM_SQUARE_VAL)/sum(FREQ)-(sum(SUM_VAL)/sum(FREQ))**2) as SD,
				sum(SUM_VAL_AMB) as COSTO_TOTALE_AMB,
				sum(SUM_VAL_RICOVERI) as COSTO_TOTALE_RICOVERI,
				sum(SUM_VAL_FARMA) as COSTO_TOTALE_FARMA,
				sum(SUM_VAL_FARMA_TERR) as COSTO_TOTALE_FARMA_TERR,
				sum(SUM_VAL_HOSPICE) as COSTO_TOTALE_HOSPICE,
				sum(SUM_VAL_PS) as COSTO_TOTALE_PS,
				sum(SUM_VAL)/(select sum(SUM_VAL) from output.stratification)*100 as PERCENTUALE_COSTO,
				sum(FREQ)/(select sum(FREQ) from output.stratification)*100 as PERCENTUALE_ASSISTITI
			from output.stratification
			group by STR_GROUP)as A
		left join output.ranking_stratification as B
			on A.STR_GROUP=B.STR_GROUP
		left join info_mono_str as C
			on A.STR_GROUP=C.STR_GROUP
		/*left join input.desc_str_group as D
			on A.STR_GROUP=D.STR_GROUP*/
		order by A.COSTO_TOTALE descending
	;
	quit;

%mend;


*%stratification_analysis();
