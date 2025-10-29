create view enrichmentdetails as
select  
    'gnb' type,
    LEFT(name, 3)  AS market,
	LEFT(name, 7)  AS gnb,
    '' AS du,
	name fullname,
	band,
	trans_cell_type transcell
from ericsson_5g_enrichment
union all
select  
    'du' type,
    LEFT(gnodeb_duid, 3)  AS market,
	LEFT(gnodeb_duid, 7)  AS gnb,
    RIGHT(gnodeb_duid, 4) AS du,
	gnodeb_duid fullname,
	band,
	trans_cell_type transcell
from samsung_5g_enrichment;
