*Algorithm*;
%macro stratified(essential_data=, comb_data=); 

		*inizializza i dati di input;	
		data data;
		set &essential_data;
		run;
		*inizializza i dati di output;
		data output.comb_str;
		set &comb_data;
		length STR_GROUP $15;
		run;

		data ranking_stratification;
		   attrib
			  ID_PATOLOGIA1 - ID_PATOLOGIA3 length=$4
			  ;
		   call missing(of _all_);
		   stop;
		run;

	*il ciclo serve a effetuare le stesse analisi per ogni patologia al netto degli assistiti precedentemente classificati;
	%do i=1 %to &NPAT;

		proc sql noprint;
			select ID_PATOLOGIA
			into: PAT&i
			from input.ranking
			where RANKING=&i;
		quit;
		%put &&PAT&i;

		*L1:calcola il rate multi per valutare se  necessario "spacchettare" la patologia e quindi effettuare le analisi successive sulle comorbidit;
		%L1(PAT=&&PAT&i, data=data);

		*************************************combinazioni a 2 *************************************************************;
		%if &rate_multi<&rate_th %then %do;
			%let c=2;
			*L2: calcola le combinazioni a 2 con il relativo impatto senza il filtro su n_pat=2;
			%L2(PAT=&&PAT&i, data=data, dataout=L2_&c._&&PAT&i);

			%if &maximpact=1 %then %do;
				*calcola info sui monopatologici per la patologia in esame (N_PAT=1);
				%info_mono_1(PAT=&&PAT&i, data=output.info_mono);
				*L3: calcola le combinazioni con n_pat=2 ed effettua i test;
				%L3(PAT=&&PAT&i, data=data, L2data=L2_&c._&&PAT&i, monodata=info_mono_1, dataout=L3_&c._&&PAT&i);

				%if &keep=1 %then %do;
					*L4:valuta se le combinazioni a 2 da mantenere vanno ulterioriormente spacchettate;
					%L4(PAT=&&PAT&i, data=data, L3data=L3_&c._&&PAT&i, dataout=L4_&c._&&PAT&i);
					*salva output delle combinazioni a 2;
					%L5(L2data=L2_&c._&&PAT&i, L3data=L3_&c._&&PAT&i, L4data=L4_&c._&&PAT&i, dataout=L5_&c);

					*************************************combinazioni a 3 ***********************************************;
					%if &maxrate=1 %then %do;

						*inizializza i dati;
						data data_&&PAT&i;
						set data;
						where &&PAT&i=1;
						run;

						%do t=1 %to &NPAT_rate;

							%let c=2;

							proc sql noprint;
								select ID_PATOLOGIA2
								into:CPAT&t
								from (select ID_PATOLOGIA2, monotonic() as ROW from output.L4_&c._&&PAT&i where FL_RATE="1")
								where ROW=&t;
							quit;
							%put &&CPAT&t;

							%let c=3;

							*L2: calcola le combinazioni a 2 con il relativo impatto senza il filtro su n_pat=2;
							%L2(PAT=&&CPAT&t, data=data_&&PAT&i, dataout=L2_&c._&&PAT&i.._&&CPAT&t);

							%if &maximpact=1 %then %do;
								*calcola info su chi ha solo le due patologie in esame (N_PAT=2);
								%info_mono_2(PAT1=&&PAT&i, PAT2=&&CPAT&t, data=&essential_data);
								*L3: calcola le combinazioni con n_pat=3 ed effettua i test;
								%L3(PAT=&&CPAT&t, data=data_&&PAT&i, L2data=L2_&c._&&PAT&i.._&&CPAT&t, monodata=info_mono_2, dataout=L3_&c._&&PAT&i.._&&CPAT&t);

								%if &keep=1 %then %do;
									*L4:valuta se le combinazioni a 2 da mantenere vanno ulterioriormente spacchettate;
									%L4(PAT=&&CPAT&t, data=data_&&PAT&i, L3data=L3_&c._&&PAT&i.._&&CPAT&t, dataout=L4_&c._&&PAT&i.._&&CPAT&t);
									*salva output delle combinazioni a 3;
									%L5(L2data=L2_&c._&&PAT&i.._&&CPAT&t, L3data=L3_&c._&&PAT&i.._&&CPAT&t, L4data=L4_&c._&&PAT&i.._&&CPAT&t, dataout=L5_&c);
									
									*inserisce dati nella tabella di ranking per le comorbidit di output;
									proc sql;
										insert into ranking_stratification
										select "&&PAT&i" as ID_PATOLOGIA1,
												"&&CPAT&t" as ID_PATOLOGIA2,
												ID_PATOLOGIA2 as ID_PATOLOGIA3
										from output.L4_3_&&PAT&i.._&&CPAT&t;
									quit;

									*compila str_group nel file di combinazioni;
									proc sql noprint;
									select count(*) 
									into: N_L4PAT
									from output.L4_&c._&&PAT&i.._&&CPAT&t;
									quit;

									%do v=1 %to &N_L4PAT;

										proc sql noprint;
										select ID_PATOLOGIA2
										into: L4PAT&v
										from output.L4_3_&&PAT&i.._&&CPAT&t
										where ranking=&v;
										quit;

										data output.comb_str;
										set output.comb_str; 
										if &&PAT&i=1 & &&CPAT&t=1 & &&L4PAT&v=1 & missing(STR_GROUP) then 
											STR_GROUP=cats("&&PAT&i", '|', "&&CPAT&t", '|', "&&L4PAT&v");		  
										run;

									%end;

								%end;
							%end;

						*aggiorna i dati al netto delle patologie gi analizzate con priorit di ranking;
						data data_&&PAT&i;
						set data_&&PAT&i;
						where &&CPAT&t=0;
						run;

						%end;

						proc delete data= data_&&PAT&i;
						run;	
					%end;

					******************************************************************************************************;
					
					*inserisce dati nella tabella di ranking per le comorbidit di output;
					proc sql;
						insert into ranking_stRatification
						select ID_PATOLOGIA as ID_PATOLOGIA1,
								ID_PATOLOGIA2,
								"" as ID_PATOLOGIA3
						from output.L4_2_&&PAT&i;
					quit;


					*compila str_group nel file di combinazioni;
					proc sql noprint;
					select count(*) 
					into: N_L4PAT
					from output.L4_2_&&PAT&i;
					quit;

					%do v=1 %to &N_L4PAT;

						proc sql noprint;
						select ID_PATOLOGIA2
						into: L4PAT&v
						from output.L4_2_&&PAT&i
						where ranking=&v;
						quit;

						data output.comb_str;
						set output.comb_str; 
						if &&PAT&i=1 & &&L4PAT&v=1 & missing(STR_GROUP) then 
							STR_GROUP=cats("&&PAT&i", '|', "&&L4PAT&v");		  
						run;

					%end;

				%end;
			%end;
		%end;
		******************************************************************************************************************;
		*inserisce dati nella tabella di ranking per le comorbidit di output;
		proc sql;
			insert into ranking_stratification
			values("&&PAT&i", "", "");
		quit;

		*compila str_group nel file di combinazioni;
		data output.comb_str;
		set output.comb_str; 
		if &&PAT&i=1 & missing(STR_GROUP) then 
			STR_GROUP="&&PAT&i";		  
		run;

		*aggiorna i dati al netto delle patologie gi analizzate con priorit di ranking;
		data data;
		set data;
		where &&PAT&i=0;
		run;


		*dm 'clear log';

	%end;
	
	*terminato il loop aggiunge alla tabella di ranking la colonna str_group;
	data  output.ranking_stratification;
	set ranking_stratification;
	if not missing(ID_PATOLOGIA3) then STR_GROUP=cats(ID_PATOLOGIA1, '|', ID_PATOLOGIA2, '|', ID_PATOLOGIA3);
	if missing(ID_PATOLOGIA2) then STR_GROUP=ID_PATOLOGIA1;
	if not missing(ID_PATOLOGIA2) & missing(ID_PATOLOGIA3) then STR_GROUP=cats(ID_PATOLOGIA1, '|', ID_PATOLOGIA2);
	STR_RANKING=_N_; 
	run;

	proc delete data= data
					ranking_stratification;
	run;

%mend;
/*
%stratified(essential_data= essential_data, comb_data= input.comb_ricod);
*/
