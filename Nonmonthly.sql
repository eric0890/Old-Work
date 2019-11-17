with
extraction as
(select
blsreg, psucoll, panel, dtcloseout, segid,
sum(case when active_ea_id <> '00151' then 1 else 0 end) as All_Fielded,
sum(case when active_ea_id <> '00151' and schedulestat <> '85' then 1 else 0 end) as Outstanding_Fielded,
sum(case when schedulestat = '85' and active_ea_id <> '00151' then 1 else 0 end) as Transmit,
sum(case when active_ea_id = '00151' then 1 else null end) as Klein,
sum(case when schedulestat = '85' and rspcodc in ('01', '04') then 1 else 0 end) as Good_Rent,
sum(case when active_ea_id = 'aaaaa' then 1 else null end) as Unassigned,
sum(case when active_ea_id = '00151' and rspcodc in ('01','04','50') then 1 else null end) as Prod_Klein,
sum(case when active_ea_id <> '00151' and schedulestat = '85' and scopstat = 'I' and rspcodc = '02' then 1 
    when active_ea_id <> '00151' and schedulestat = '85' and scopstat = 'M' and ssrsn like 'UC%' then 1 
    when active_ea_id <> '00151' and schedulestat = '85' and scopstat = 'M' and ssrsn = 'DK' then 1
    when schedulestat = '85' and rspcodc = '50' then 1
    else null end) as UCDKVacant_Transmit
from hsofo_dcqp.schedule_housing1_ofo 
where 
collper = '201812'
And Fg_Non_Monthly = 'Y'
group by blsreg, psucoll, panel, dtcloseout, segid
order by blsreg, Psucoll, panel, segid)

,seg_sufficiency as
(select blsreg,psucoll, panel, segid,
sum(good_rent) as good_rent,
case when sum(good_rent) >= 5 then 'green'
     when sum(good_rent) <  2 then 'red'
     when sum(good_rent) between 2 and 4 then 'yellow'
     else null end as Display_Color,
sum(Case When good_rent >= 2 Then 1 Else 0 End) As segwith2,
sum(case when good_rent >= 5 then 1 else 0 end) as Suff_Seg,
sum(case when good_rent >= 8 then 1 else 0 end) as Suff160_Seg,
sum(case when good_rent >= 0 then 1 else 0 end) as zero,
sum(case when good_rent >= 1 then 1 else 0 end) as one,
sum(case when good_rent >= 2 then 1 else 0 end) as two,
sum(case when good_rent >= 3 then 1 else 0 end) as three,
sum(case when good_rent >= 4 then 1 else 0 end) as four,
sum(case when good_rent >= 5 then 1 else 0 end) as five
from extraction e
group by blsreg, psucoll, panel, segid
order by blsreg, psucoll, panel, segid)  

,fielded160 as
(select blsreg, psucoll, panel,
sum(outstanding_fielded) as suff160_units,
LISTAGG(segid, ', ') WITHIN GROUP (ORDER BY segid) AS Suff160_Seg
from extraction
where good_rent >= '8'
and outstanding_fielded > '0'
group by blsreg, psucoll, panel
order by blsreg, psucoll, panel)

,klein_insuff as
(select blsreg, psucoll, panel,
sum(Klein) as klein_insuff_units,
LISTAGG(segid, ', ') WITHIN GROUP (ORDER BY segid) AS klein_insuff_seg
from extraction
where good_rent < '5'
and klein >= '1'
group by blsreg, psucoll, panel
order by blsreg, psucoll, panel) 

,UCDKVACANT_TRANSMIT as
(select blsreg,psucoll, panel,
sum(UCDKVacant_Transmit) as UCDKVacant_Units,
LISTAGG(segid, ', ') WITHIN GROUP (ORDER BY segid) AS UCDKVacant_Seg
from extraction
where UCDKVacant_Transmit >= '1'
group by blsreg, psucoll, panel
order by blsreg, psucoll, panel) 

,Prod_Klein as
(select blsreg, psucoll, panel,
sum(Prod_Klein) as ProdKlein_Units,
LISTAGG(segid, ', ') WITHIN GROUP (ORDER BY segid) AS ProdKlein_Seg
from extraction
where Prod_Klein >= '1'
group by blsreg, psucoll, panel
order by blsreg, psucoll, panel) 

,aggregation as 
(select 
e.blsreg, 
e.psucoll,
e.panel,
e.dtcloseout,
count(e.segid) as seg_count,
sum(s.segwith2) as segwith2,
sum(s.suff_seg) as suff_seg,
sum(s.suff160_seg) as suff160_seg,
sum(all_Fielded) as fielded,
case when sum(outstanding_Fielded) > '0' then sum(outstanding_Fielded) else null end as Outstanding,
sum(transmit) as transmitted,
case when sum(e.Good_Rent) > '0' then sum(e.good_rent) else 0 end as Rents,
(5*count(e.segid)) as total_needed,
(case when (5*count(e.segid)) - sum(e.good_rent) >= '1' then (5*count(e.segid)) - sum(e.good_rent) else 0 end) as units_to_100,
sum(e.klein) as Total_Klein,
sum(e.unassigned) as unassigned,
f160.suff160_units as fielded160,
ki.klein_insuff_units as klein_insuff_units,
sum(e.Prod_Klein) as prod_klein,
sum(UCDKVacant_Transmit) as UCDKVacant_Transmit
from extraction e
inner join seg_sufficiency s on e.psucoll = s.psucoll and e.segid = s.segid and e.panel = s.panel
left join klein_insuff ki on e.psucoll = ki.psucoll and e.panel = ki.panel
left join fielded160 f160 on e.psucoll = f160.psucoll and e.panel = f160.panel
GROUP BY e.BLSREG, E.psucoll, e.panel, e.dtcloseout,f160.suff160_units, ki.klein_insuff_units
order by e.blsreg, e.psucoll, e.panel)

SELECT distinct
a.blsreg,
a.psucoll,
a.panel,
a.dtcloseout, 
round(100* (a.transmitted / a.fielded),1) || '%' as trans,
(Case When (100*(a.rents / a.total_needed)) > 0 Then round(100*(a.rents / a.total_needed),1) Else 0 End) || '%' as Sufficiency,
a.segwith2|| ' / ' || a.seg_count as Seg2_Ratio,
round(100*(a.segwith2/a.seg_count),0) || '%' as Seg2_Prct,
a.total_needed,
a.rents,
a.Units_to_100,
a.fielded,
a.transmitted,
a.outstanding,
a.unassigned,
a.total_klein,
case when a.fielded160 > '0' then a.fielded160 || ' Units: ' || f160.Suff160_Seg else null end as Fielded160,
case when a.Klein_Insuff_Units > '0' then a.Klein_Insuff_Units || ' Units: ' || ki.klein_insuff_seg else null end as Klein_Insuff,
case when a.prod_klein > '0' then a.prod_klein|| ' Units: ' || p.prodklein_seg else null end as Klein_Prod,
case when a.UCDKVacant_Transmit > '0' then a.UCDKVacant_Transmit || ' Units: ' || e.UCDKVacant_Seg else null end as Early_Transmit
FROM AGGREGATION a
left join fielded160 f160 on f160.psucoll = a.psucoll and f160.panel = a.panel
left join Klein_insuff ki on ki.psucoll = a.psucoll and ki.panel = a.panel
left join Prod_Klein p on p.psucoll = a.psucoll and p.panel = a.panel
left join UCDKVACANT_TRANSMIT e on e.psucoll = a.psucoll and e.panel = a.panel

UNION ALL

SELECT 
coalesce(blsreg, 'All') as blsreg, 
'' as psucoll,
coalesce(panel, 'All') as panel, 
max(a.dtcloseout) as dtcloseout, 
round(100*(sum(a.transmitted) / sum(a.fielded)),1) || '%' as trans,
(Case When (100*(sum(a.rents) / sum(a.total_needed))) > 0 Then round(100*(sum(a.rents) / sum(a.total_needed)),1) Else 0 End) || '%' as Sufficiency,
sum(a.segwith2)|| ' / ' || sum(a.seg_count) as Seg2_Ratio,
round(100*(sum(a.segwith2)/sum(a.seg_count)),0) || '%' as Seg2_Prct,
sum(a.total_needed) as total_needed,
sum(a.rents) as rents,
sum(a.Units_to_100) as units_to_100,
sum(a.fielded) as fielded,
sum(a.transmitted)as transmitted,
sum(a.outstanding) as outstanding,
sum(a.unassigned) as unassigned,
sum(a.total_klein) as total_klein,
case when sum(a.fielded160) > '0' then sum(a.fielded160) || ' Units' else null end as Fielded160,
case when sum(a.Klein_Insuff_Units) > '0' then sum(a.Klein_Insuff_Units) || ' Units' else null end as Klein_Insuff,
case when sum(a.prod_klein) > '0' then sum(a.prod_klein) || ' Units' else null end as Klein_Prod,
case when sum(a.UCDKVacant_Transmit) > '0' then sum(a.UCDKVacant_Transmit) || ' Units' else null end as Early_Transmit
FROM AGGREGATION a
group by rollup(blsreg, panel)

UNION ALL

SELECT 
'All' as blsreg, 
'All' as psucoll,
panel as panel, 
max(a.dtcloseout) as dtcloseout, 
round(100*(sum(a.transmitted) / sum(a.fielded)),1) || '%' as trans,
(Case When (100*(sum(a.rents) / sum(a.total_needed))) > 0 Then round(100*(sum(a.rents) / sum(a.total_needed)),1) Else 0 End) || '%' as Sufficiency,
sum(a.segwith2)|| ' / ' || sum(a.seg_count) as Seg2_Ratio,
round(100*(sum(a.segwith2)/sum(a.seg_count)),0) || '%' as Seg2_Prct,
sum(a.total_needed) as total_needed,
sum(a.rents) as rents,
sum(a.Units_to_100) as units_to_100,
sum(a.fielded) as fielded,
sum(a.transmitted) as transmitted,
sum(a.outstanding) as outstanding,
sum(a.unassigned) as unassigned,
sum(a.total_klein) as total_klein,
case when sum(a.fielded160) > '0' then sum(a.fielded160) || ' Units' else null end as Fielded160,
case when sum(a.Klein_Insuff_Units) > '0' then sum(a.Klein_Insuff_Units) || ' Units' else null end as Klein_Insuff,
case when sum(a.prod_klein) > '0' then sum(a.prod_klein) || ' Units' else null end as Klein_Prod,
case when sum(a.UCDKVacant_Transmit) > '0' then sum(a.UCDKVacant_Transmit) || ' Units' else null end as Early_Transmit
FROM AGGREGATION a
group by panel

order by blsreg asc, psucoll asc, panel;