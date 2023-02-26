%macro info(data_in=, data_out=);
	
	proc sql;
	create table output.&data_out._tot as
	select count(*) as freq, sum(costo) as val, sum(costo)/(calculated freq) as costo_medio
	from &data_in
	run;
	
	data _null_;
	set output.&data_out._tot;
	call symputx('freq_tot', freq);
	run;
	%put frequenza totale: &Freq_tot;

	proc sql;
	create table output.&data_out as
	select cod_regione, 
			eta, 
			sesso, 
			count(*) as freq, 
			sum(costo) as val, 
			sum(costo)/(calculated freq) as costo_medio, 
			(calculated freq)/&freq_tot as freq_rel
	from &data_in 
	group by cod_regione, eta, sesso;
	run;
	
	proc sql;
	create table output.&data_out._reg as
	select cod_regione, 
			count (*) as freq, 
			sum(costo) as val, 
			sum(costo)/(calculated freq) as costo_medio, 
			(calculated freq)/&freq_tot as freq_rel
	from &data_in 
	group by cod_regione;
	run;

	proc sql;
	create table output.&data_out._sesso as
	select sesso, 
			count(*) as freq, 
			sum(costo) as val, 
			sum(costo)/(calculated freq) as costo_medio, 
			(calculated freq)/&freq_tot as freq_rel
	from &data_in 
	group by sesso;
	run;
	
	proc sql;
	create table output.&data_out._eta as
	select eta, 
			count(*) as freq, 
			sum(costo) as val, 
			sum(costo)/(calculated freq) as costo_medio, 
			(calculated freq)/&freq_tot as freq_rel
	from &data_in 
	group by eta;
	run;

%mend;
