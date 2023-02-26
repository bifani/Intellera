%macro info_chronic(data=);
proc sql;
		create table output.info_chronic as
			select
			sum(SUM_VAL) as VAL,
			sum(FREQ) as FREQ,
			sum(SUM_VAL)/sum(FREQ) AS MEAN_VAL
		from &data;
quit;
%mend;

*%info_chronic(data=input.essential_data);

%macro info_mono(data=);

	data output.info_mono (keep=ID_PATOLOGIA FREQ VAL MEAN_VAL VARIANCE SD);
	set &data;
	length ID_PATOLOGIA $4;
	array VAR &PATsas;
		do over VAR;
	       if VAR=1 & N_PAT=1 then do;
			   ID_PATOLOGIA=compress(vname(VAR));
			   VAL=SUM_VAL;
			   MEAN_VAL=SUM_VAL/FREQ;
			   VARIANCE=sum(SUM_SQUARE_VAL/FREQ,-(SUM_VAL/FREQ)**2);
			   SD=sqrt(VARIANCE);
		   end;
	    end;
	where N_PAT=1;
	run;

%mend;

*%info_mono(data=input.essential_data);



%macro info_mono_1 (PAT=, data=);
	data info_mono_1;
	set &data;
	where ID_PATOLOGIA="&&PAT";
	run;
%mend;


*%info_mono_1(PAT=CA05, data=output.info_mono);


%macro info_mono_2(PAT1=, PAT2=, data=);

	data info_mono_2 (keep=ID_PATOLOGIA ID_PATOLOGIA2 FREQ VAL MEAN_VAL VARIANCE SD);
	set &data;
	ID_PATOLOGIA="&&PAT1";
	ID_PATOLOGIA2="&&PAT2";
	VAL=SUM_VAL;
	MEAN_VAL=SUM_VAL/FREQ;
	VARIANCE=sum(SUM_SQUARE_VAL/FREQ,-(SUM_VAL/FREQ)**2);
	SD=sqrt(VARIANCE);
	where &PAT1=1 & &PAT2=1 & N_PAT=2;
	run;

%mend;

*%info_mono_2(PAT1=CA05, PAT2=NE01, data=input.essential_data);


%macro impact(data=);

	%do i=1 %to &NPAT;

		proc sql noprint;
			select id_patologia
			into: PAT&i
			from (select ID_PATOLOGIA, monotonic() as ROW from input.area_patologica)
		where ROW=&i;
		quit;

		proc sql;
			create table temporary&i as
			select "&&PAT&i" as ID_PATOLOGIA,
					sum(SUM_VAL) as VAL,
					sum(FREQ) as FREQ,
					sum(SUM_VAL)/sum(FREQ) as MEAN_VAL,
					(sum(SUM_VAL)/(select VAL from output.info_chronic))*100 as PERC_IMPACT,
					(select MEAN_VAL from output.info_mono where ID_PATOLOGIA="&&PAT&i")/calculated MEAN_VAL as RATE_MULTI, /*non  calcolato al netto delle patologie che hanno priorit*/
					case when calculated RATE_MULTI<&rate_th then "1"
						else "0" end as FL_RATE
			from (select * from &data where &&PAT&i=1);
		quit;

	%end;

	%let q=%eval(&i-1);

	data output.impact;
	set temporary1 - temporary&q;
	run;

	proc sort data=output.impact;
	by descending PERC_IMPACT;
	run;

	proc delete data= temporary1 - temporary&q;
	run;

%mend;


*%impact(data=input.essential_data);
