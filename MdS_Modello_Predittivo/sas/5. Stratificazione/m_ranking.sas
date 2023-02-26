%macro ranking(data=, var=);

	proc sort data= &data;
	by descending &var;
	where ID_PATOLOGIA <> 'AL05';
	run;

	data input.ranking (keep= ID_PATOLOGIA &var RANKING);
	set &data;
	RANKING=_N_;
	run;
	
	proc sql;
 	select
 	MAX(RANKING)+1 into :RANK_AL05
	from input.ranking ;
 	quit; 

	proc sql;
 	select
 	MEAN_VAL into :MEAN_AL05
	from &data ;
 	quit;

	proc sql;
	insert into input.ranking (ID_PATOLOGIA, MEAN_VAL ,RANKING) 
	values ('AL05',&MEAN_AL05,&RANK_AL05);
	quit;

%mend;


*%ranking(data=output.info_mono, var=mean_val);
