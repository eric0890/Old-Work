--*Housing Data (TEST)*
with Housing as
(select 
a.psuix || ' - ' || a.segid || ' - ' || a.lineid as PSU_SEG_LINE,
c.utseno || ' ' || c.utsena as Address,
c.utcity as City, c.utstate as State, c.utzip as Zip, a.collper, a.panel,
a.psucoll as coll_psu, a.dtcloseout as Closeout, 
a.active_ea_id as OFOID, c.latitude, c.longitude, c.version
from hsofo_dcqp.schedule_housing1_ofo a, hsprod.schedule_housing2 c
where a.collper = '201811'
and a.fg_non_monthly = 'N'
and a.psuix=c.psuix    
and a.segid=c.segid 
and a.lineid=c.lineid 
and c.schedule_usage_type = 'DC'
and a.collper=c.collper
and a.OFO_EA_ID <> '00151'
and psucoll = 'N902'
order by latitude)
select *
from housing h
where version = (select max(version) 
from housing h2 
where h.psu_seg_line = h2.Psu_seg_line);
===============================================================================
--*Housing Data to seperate mid-cycle, collected vs not collected....version variable*
select a.psuix || ' - ' || a.segid || ' - ' || a.lineid as PSU_SEG_LINE,
c.utseno || ' ' || c.utsena as Address,
c.utcity as City, c.utstate as State, c.utzip as Zip,
a.version, a.ofo_ea_id, a.psucoll, a.dttm_arrival, a.inttype, c.latitude, c.longitude  
from hsofo_dcqp.schedule_housing1_ofo a, hsofo_dcqp.schedule_housing2_ofo c
where a.collper = '201811' 
and a.fg_non_monthly = 'N'
and a.psucoll = 'N902' 
and a.psuix=c.psuix    
and a.segid=c.segid 
and a.lineid=c.lineid 
and a.collper=c.collper
and a.ofo_ea_id <> '00151'
and a.version = '2'
;
