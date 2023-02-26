%macro L1(PAT=, data=);
	proc sql noprint; 
		select 
			(select MEAN_VAL from output.info_mono where ID_PATOLOGIA="&PAT")/MEAN_VAL into: rate_multi
		from (select sum(SUM_VAL)/sum(FREQ) as MEAN_VAL from &data where &PAT=1)
	quit;
	%put &rate_multi;
%mend;

/*
%L1(PAT=&&PAT&i, data=data);
*/
/*
esempio:
%L1(PAT=CA05, data=essential_data);
*/

%macro L2(PAT=, data=,  dataout=);

	%do j=1 %to &NPAT;

		proc sql noprint;
			select id_patologia
			into: PT&j 
			from (select ID_PATOLOGIA, monotonic() as ROW from input.area_patologica)
		where ROW=&j;
		quit;

 
		proc sql; 
			create table temporary&j as
			select "&PAT" as ID_PATOLOGIA,
					"&&PT&j" as ID_PATOLOGIA2,
					sum(FREQ) as FREQ, 
					sum(SUM_VAL) as VAL,
					sum(SUM_VAL)/sum(FREQ) as MEAN_VAL,
					sum(sum(SUM_SQUARE_VAL)/sum(FREQ),-(sum(SUM_VAL)/sum(FREQ))**2) as VARIANCE,
				    sqrt(calculated VARIANCE) as SD,
					(sum(SUM_VAL)/(select VAL from output.info_chronic))*100 as PERC_IMPACT,
					case when calculated PERC_IMPACT> &impact_th then "1" 
						else "0" end as FL_IMPACT 	
			from (select * from &data where &PAT=1 & &&PT&j=1);
		quit;

	%end;

	%let q=%eval(&j-1);

	data output.&dataout;
	set temporary1 - temporary&q;
	run;

	proc sort data=output.&dataout;
	by descending PERC_IMPACT;
	run;

	proc delete data= temporary1 - temporary&q;
	run;

	%if &c=3 %then %do;
		data output.&dataout;
		set output.&dataout;
		where ID_PATOLOGIA2 <> "&&PAT&i";
		run;
	%end;

	proc sql noprint;
		select max(input(FL_IMPACT, 1.))
		into: maximpact
		from output.&dataout
		where ID_PATOLOGIA ne ID_PATOLOGIA2;
	quit;

%mend;

/*
%L2(PAT=&&PAT&i, data=data, dataout=L2_&&PAT&i);
%L2(PAT=&&CPAT&t, data=data_&&PAT&i, dataout=%cmpres(L2_&c._&&PAT&i.._&&CPAT&t));
*/
/*
esempio:
%L2(PAT=CA05, data=essential_data, dataout=L2_2_CA05);

%let c=3;
%let i=5;
%let t=1;
%let PAT&i=CA05;
%let CPAT&t=NE01;
%L2(PAT=&&CPAT&t, data=essential_data, dataout=L2_&c._&&PAT&i.._&&CPAT&t)
*/


%macro L3(PAT=, data=, L2data=, monodata=, dataout=);


		data &dataout._0(keep=ID_PATOLOGIA ID_PATOLOGIA2 FREQ VAL MEAN_VAL VARIANCE SD);
		set &data;
		length ID_PATOLOGIA2 $4;
		array VAR &PATsas;
			do over VAR;
		       if &PAT=1 & VAR=1 & N_PAT=&c & vname(VAR) ne "&PAT" then do;
			   ID_PATOLOGIA="&PAT";
			   ID_PATOLOGIA2=compress(vname(VAR));
			   VAL=SUM_VAL;
			   MEAN_VAL=SUM_VAL/FREQ;
			   VARIANCE=sum(SUM_SQUARE_VAL/FREQ,-(SUM_VAL/FREQ)**2);
			   SD=sqrt(VARIANCE);
			   end;
		    end;
		where N_PAT=&c & &PAT=1 ;
		run;
	
		proc sql;
			create table output.&dataout as
			select a.*,
				a.MEAN_VAL/(select MEAN_VAL from &monodata) as RATE_MONO,
				sqrt((a.VARIANCE+(select VARIANCE from &monodata))/2)as SD_POOLED,
				(a.MEAN_VAL-(select MEAN_VAL from &monodata))/(calculated SD_POOLED) as HEDGES_TEST,
				case when calculated HEDGES_TEST> &hedges_th then "1" 
					else "0" end as FL_TEST,
				b.FL_IMPACT,
				case when FL_IMPACT="1" & calculated FL_TEST="1" then "1"
				else "0" end as FL_KEEP
			from &dataout._0 as a
			left join output.&L2data as b
			on a.ID_PATOLOGIA2=b.ID_PATOLOGIA2
			order by a.MEAN_VAL descending;
		quit;

		proc delete data=&dataout._0;
		run;
		
		%if &c=3 %then %do;
			data output.&dataout;
			set output.&dataout;
			where ID_PATOLOGIA2 <> "&&PAT&i";
			run;
		%end;

		proc sql noprint;
			select max(input(FL_KEEP, 1.))
			into: keep
			from output.&dataout;
		quit;
%mend;

/*
%L3(PAT=&&PAT&i, data=data, L2data=L2_&&PAT&i, monodata=output.info_mono, dataout=L3_&&PAT&i);
%L3(PAT=&&CPAT&t, data=data_&&PAT&i, L2data=L2_&c._&&PAT&i.._&&CPAT&t, monodata=info_mono_2, dataout=L3_&c._&&PAT&i.._&&CPAT&t);
*/

/*
esempio:
%let c=2;
%info_mono_1(PAT=CA05, data=output.info_mono);
%L3( PAT=CA05, data=essential_data, L2data=L2_2_CA05, monodata=info_mono_1, dataout=L3_2_CA05);
**********;
*inizializza i dati;
data data_CA05;
set essential_data;
where CA05=1;
run;

%let c=3;
%info_mono_2(PAT1=CA05, PAT2=NE01, data=essential_data);
%L3(PAT=NE01, data=data_CA05, L2data=L2_3_CA05_NE01, monodata=info_mono_2, dataout=L3_3_CA05_NE01);
*/


%macro L4(PAT=, data=, L3data=, dataout=);

	proc sql noprint;
 	select count(*) into :Nkeep 
	from output.&L3data
	where FL_KEEP='1';
	quit;
	
	%do j=1 %to &Nkeep;

	%if &j=1 %then %do;
		%let data&j=&data;
		%end;
		%else %do;
		%let data&j=data_new;
		%end;

	proc sql noprint;
		select ID_PATOLOGIA2
		into:PT&j
		from (select ID_PATOLOGIA2, monotonic() as ROW from output.&L3data where FL_KEEP="1")
		where ROW=&j;
	quit;
/*
		proc sql; 
			create table temporary&j as
			select "&PAT" as ID_PATOLOGIA,
					"&&PT&j" as ID_PATOLOGIA2,
					MEAN_VAL/(select sum(SUM_VAL)/sum(FREQ) as MEAN_VAL from &&data&j where &PAT=1 & &&PT&j=1) as RATE_MULTI,
					case when calculated RATE_MULTI<&rate_th then "1" 
						else "0" end as FL_RATE
			from (select * from output.&L3data where ID_PATOLOGIA2="&&PT&j");
		quit;

*/
/*modifica nuova 10102022*/
	proc sql; 
			create table temporary&j as
			select "&PAT" as ID_PATOLOGIA,
					"&&PT&j" as ID_PATOLOGIA2,
					MEAN_VAL/(select sum(SUM_VAL)/sum(FREQ) as MEAN_VAL from input.essential_data where &PAT=1 & &&PT&j=1) as RATE_MULTI,
					case when calculated RATE_MULTI<&rate_th_l4 then "1" 
						else "0" end as FL_RATE
			from (select * from output.&L3data where ID_PATOLOGIA2="&&PT&j");
		quit;

		data data_new;
		set &&data&j;
		where &&PT&j=0;
		run;

		%end;

	%let q=%eval(&j-1);

	data output.&dataout;
	set temporary1 - temporary&q;
	ranking=_N_;
	run;

	proc delete data= temporary1 - temporary&q;
	run;

	proc sql noprint;
		select max(input(FL_RATE,1.))
		into: maxrate
		from output.&dataout;
	quit;

	proc sql noprint;
		select count(id_patologia)
		into: NPAT_rate 
		from output.&dataout
		where FL_RATE="1";
	quit;

%mend;


%macro L5(L2data=, L3data=, L4data=, dataout=);

	%if %sysfunc(exist(output.&dataout))=0 %then %do;
		data output.&dataout;
		   attrib
			  ID_PATOLOGIA1 - ID_PATOLOGIA&c length=$4
			  RANKING length=8
			  FREQ length=8
			  VAL length=8
			  MEAN_VAL length=8
			  VARIANCE length=8
			  SD length=8
			  PERC_IMPACT length=8
			  RATE_MULTI length=8
			  FL_RATE length=$1
			  ;
		   call missing(of _all_);
		   stop;
		run;
	%end;

	%if &c=2 %then %do;
		proc sql;
		create table &dataout._0 as
			select a.ID_PATOLOGIA1,
				a.ID_PATOLOGIA2,
				c.RANKING,	
				b.FREQ,
				b.VAL,
				b.MEAN_VAL,
				b.VARIANCE,
				b.SD,
				b.PERC_IMPACT,
				C.RATE_MULTI,
				C.FL_RATE
			from (select  ID_PATOLOGIA as ID_PATOLOGIA1, ID_PATOLOGIA2, mean_val from output.&L3data where FL_KEEP="1") as a
			left join output.&L2data as b
			on a.ID_PATOLOGIA1=b.ID_PATOLOGIA & a.ID_PATOLOGIA2=b.ID_PATOLOGIA2
			left join output.&L4data as c
			on a.ID_PATOLOGIA1=c.ID_PATOLOGIA & a.ID_PATOLOGIA2=c.ID_PATOLOGIA2
			order by a.mean_val desc;
		quit;
	%end;
	%if &c=3 %then %do;
		proc sql;
		create table &dataout._0 as
			select 
				a.ID_PATOLOGIA1,
				a.ID_PATOLOGIA2,
				a.ID_PATOLOGIA3,
				c.RANKING,
				b.FREQ,
				b.VAL,
				b.MEAN_VAL,
				b.VARIANCE,
				b.SD,
				b.PERC_IMPACT,
				C.RATE_MULTI,
				C.FL_RATE
			from (select "&&PAT&i" as ID_PATOLOGIA1, ID_PATOLOGIA as ID_PATOLOGIA2, ID_PATOLOGIA2 as ID_PATOLOGIA3, mean_val from output.&L3data where FL_KEEP="1") as a
			left join output.&L2data as b
			on a.ID_PATOLOGIA2=b.ID_PATOLOGIA & a.ID_PATOLOGIA3=b.ID_PATOLOGIA2
			left join output.&L4data as c
			on a.ID_PATOLOGIA2=c.ID_PATOLOGIA & a.ID_PATOLOGIA3=c.ID_PATOLOGIA2
			order by a.mean_val desc;
		quit;
	%end;

	proc append  base=output.&dataout data=&dataout._0 force;
	run;

	proc delete data=&dataout._0;
	run;
				
*qui sarebbe utile partire gi� da dei dataset con il giusto nome di id_patologia;
		/*	proc sql;
				insert into output.&dataout
					select a.*,
					c.RANKING,
					b.FREQ,
					b.VAL,
					b.MEAN_VAL,
					b.VARIANCE,
					b.SD,
					b.PERC_IMPACT,
					C.RATE_MULTI,
					C.FL_RATE
				from (select  ID_PATOLOGIA, ID_PATOLOGIA2 from output.&L3data where FL_KEEP="1") as a
				left join output.&L2data as b
				on a.ID_PATOLOGIA=b.ID_PATOLOGIA & a.ID_PATOLOGIA2=b.ID_PATOLOGIA2
				left join output.&L4data as c
				on a.ID_PATOLOGIA=c.ID_PATOLOGIA & a.ID_PATOLOGIA2=c.ID_PATOLOGIA2;
			run;*/	

%mend;
/*
%L5(L2data=L2_&c._&&PAT&i, L3data=L3_&c._&&PAT&i, L4data=L4_&c._&&PAT&i, dataout=L5_&c);
%L5(L2data=L2_&c._&&PAT&i.._&&CPAT&t, L3data=L3_&c._&&PAT&i.._&&CPAT&t, L4data=L4_&c._&&PAT&i.._&&CPAT&t, dataout=L5_&c);									
*/

/*esempio:
%let c=2;
%L5(L2data=L2_2_CA05, L3data=L3_2_CA05, L4data=L4_2_CA05, dataout=L5_2);
%let c=3;
%L5(L2data=L2_3_CA05_NE01, L3data=L3_3_CA05_NE01, L4data=L4_3_CA05_NE01, dataout=L5_3);
*/
