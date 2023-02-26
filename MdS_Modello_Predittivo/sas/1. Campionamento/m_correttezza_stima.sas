%macro valuta_camp(info_dt_pop=, info_dt_camp=, camp=);

proc sql;
create table output.correttezza_val&camp as
select a.*, b.freq as freq_camp, b.val as val_camp, b.costo_medio as costo_medio_camp,
		(b.costo_medio-a.costo_medio) as err_costo_medio,
		(b.costo_medio-a.costo_medio)/a.costo_medio as err_rel_costo_medio,
		abs((b.costo_medio-a.costo_medio)/a.costo_medio) as err_rel_costo_medio_ass
from &info_dt_pop._tot as a
cross join &info_dt_camp._tot as b;
run;

proc sql;
create table output.correttezza_reg_eta_sesso&camp as
select a.*, b.freq as freq_camp, b.val as val_camp, b.costo_medio as costo_medio_camp, b.freq_rel as freq_rel_camp,
		(b.costo_medio-a.costo_medio) as err_costo_medio,(b.freq_rel-a.freq_rel) as err_freq_rel,
		(b.costo_medio-a.costo_medio)/a.costo_medio as err_rel_costo_medio,(b.freq_rel-a.freq_rel)/a.freq_rel as err_rel_freq_rel,
		abs((b.costo_medio-a.costo_medio)/a.costo_medio) as err_rel_costo_medio_ass,abs((b.freq_rel-a.freq_rel)/a.freq_rel) as err_rel_freq_rel_ass
from &info_dt_pop as a
left join &info_dt_camp as b
	on a.cod_regione=b.cod_regione and a.eta=b.eta and a.sesso=b.sesso;
run;

proc sql;
create table output.correttezza_reg&camp as
select a.*, b.freq as freq_camp, b.val as val_camp, b.costo_medio as costo_medio_camp, b.freq_rel as freq_rel_camp,
		(b.costo_medio-a.costo_medio) as err_costo_medio,(b.freq_rel-a.freq_rel) as err_freq_rel,
		(b.costo_medio-a.costo_medio)/a.costo_medio as err_rel_costo_medio,(b.freq_rel-a.freq_rel)/a.freq_rel as err_rel_freq_rel,
		abs((b.costo_medio-a.costo_medio)/a.costo_medio) as err_rel_costo_medio_ass,abs((b.freq_rel-a.freq_rel)/a.freq_rel) as err_rel_freq_rel_ass
from &info_dt_pop._reg as a
left join &info_dt_camp._reg as b
	on a.cod_regione=b.cod_regione;
run;

proc sql;
create table output.correttezza_eta&camp as
select a.*, b.freq as freq_camp, b.val as val_camp, b.costo_medio as costo_medio_camp, b.freq_rel as freq_rel_camp,
		(b.costo_medio-a.costo_medio) as err_costo_medio,(b.freq_rel-a.freq_rel) as err_freq_rel,
		(b.costo_medio-a.costo_medio)/a.costo_medio as err_rel_costo_medio,(b.freq_rel-a.freq_rel)/a.freq_rel as err_rel_freq_rel,
		abs((b.costo_medio-a.costo_medio)/a.costo_medio) as err_rel_costo_medio_ass,abs((b.freq_rel-a.freq_rel)/a.freq_rel) as err_rel_freq_rel_ass
from &info_dt_pop._eta as a
left join &info_dt_camp._eta as b
	on a.eta=b.eta;
run;

proc sql;
create table output.correttezza_sesso&camp as
select a.*, b.freq as freq_camp, b.val as val_camp, b.costo_medio as costo_medio_camp, b.freq_rel as freq_rel_camp,
		(b.costo_medio-a.costo_medio) as err_costo_medio,(b.freq_rel-a.freq_rel) as err_freq_rel,
        (b.costo_medio-a.costo_medio)/a.costo_medio as err_rel_costo_medio,(b.freq_rel-a.freq_rel)/a.freq_rel as err_rel_freq_rel,
		abs((b.costo_medio-a.costo_medio)/a.costo_medio) as err_rel_costo_medio_ass,abs((b.freq_rel-a.freq_rel)/a.freq_rel) as err_rel_freq_rel_ass
from &info_dt_pop._sesso as a
left join &info_dt_camp._sesso as b
	on a.sesso=b.sesso;
run;

%mend;

%macro plot(dt_pop=, dt_camp=, var=);
	proc sql;
	   create table unione_pop_camp as
	      select *, "pop" as fonte from &dt_pop
	      union all
	      select *, "camp" as fonte from &dt_camp;
	quit;
	 
	proc sgplot data=unione_pop_camp;
	title "Distribuzione di &var nella popolazione"; 
	vbar &var //*group=fonte groupdisplay=cluster transparency=0.5*/stat=Percent fillattrs=(color=CX191970);
	where fonte='pop';

	run;

	proc sgplot data=unione_pop_camp;
	title "Distribuzione di &var nel campione"; 
	vbar &var //*group=fonte groupdisplay=cluster*/ stat=Percent fillattrs=(color=CX191970);
	where fonte='camp';
	run;

%mend;
