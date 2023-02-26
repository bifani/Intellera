%macro macrovar_list_creation();

proc sql noprint;
	select distinct (id_patologia)
	into: PATsas separated by ' '	
from input.area_patologica;
run;
%put &PATsas;

proc sql noprint;
	select distinct (id_patologia)
	into: PATsql separated by ','	
from input.area_patologica;
run;
%put &PATsql;

proc sql noprint;
	select count(*)
	into: NPAT 
from input.area_patologica;
run;
%put &NPAT;

%mend;

*%macrovar_list_creation();
