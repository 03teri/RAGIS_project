--намиране на дистнацията до най-близката метростнация до всеки медицински център
drop table  gis_tereza.closest_metrostation_to_medcenter ;
create table gis_tereza.closest_metrostation_to_medcenter as
select * from (
	select
		mcg.id as medcentr_id, mcg.geom as medcentr_geom, mcg.title as medcentr_title,
		msp.id  as metrostation_id, msp.geom as metrotstation_geom, msp.name as metrostation_name,
	--	st_distance(st_dump(msp.geom)), mcg.geom) as distance,
		rank() over (
		   partition by mcg.id  
		--   order by msp.geom <-> ms.geom asc ) rnk
		   order by mcg.geom <-> (st_dump(msp.geom)).geom asc) rnk
	from
		med_centers_gmaps_2023 mcg, 
		gis_tereza.metro_spirki_point msp) inr where inr.rnk = 1;
	
--създаване на таблица, който съдържа само id, геометрия и име на метростанциите, които са близо до медицинските центрове
drop table  gis_tereza.metrostations_close_to_med_centers;
create table gis_tereza.metrostations_close_to_med_centers as
select distinct cmtm.metrostation_id as metrostation_id, cmtm.metrotstation_geom as metrostation_geom, cmtm.metrostation_name as metrostation_name
from gis_tereza.metro_spirki_point msp  , gis_tereza.closest_metrostation_to_medcenter cmtm 
where msp.id = cmtm.metrostation_id 
order by cmtm.metrostation_id ;
	
--за всяка метростанция казва колко са най-близките медицински центрове до нея. (Брой броя на срещанията на id на метростанциите в closest_metrostation_to_medcenters)	
drop table gis_tereza.count_med_centerts_to_every_metrostation;
create table gis_tereza.count_med_centerts_to_every_metrostation as
select count (*),cmtm.metrostation_id
from gis_tereza.closest_metrostation_to_medcenter cmtm
group by cmtm.metrostation_id 
order by cmtm.metrostation_id ;

--намиране на броя на метростанциите в София. Това е само за проба, после го изтрии
drop table gis_tereza.ranking_metrostation_to_medcenters ;
create table gis_tereza.ranking_metrostation_to_medcenters as
select 
	mcg.id as medcentr_id, mcg.geom as medcentr_geom, mcg.title as medcentr_title,
	msp.id  as metrostation_id, msp.geom as metrotstation_geom,
	rank() over (
	   partition by mcg.id  
		--   order by msp.geom <-> ms.geom asc ) rnk
	   order by mcg.geom <-> (st_dump(msp.geom)).geom asc) rnk
from
		med_centers_gmaps_2023 mcg, 
		gis_tereza.metro_spirki_point msp;
	


	

	
	-- to test, not in use
	select cmtm.medcentr_id , cmtm.metrostation_id 
	from gis_tereza.closest_metrostation_to_medcenter cmtm
	where metrostation_id = 46;
	
	
	--to test
	--convert mulptipoint to point
	select (st_dump(msp.geom)).geom as res
	from gis_tereza.metro_spirki_point msp;
	
	 --work
	select st_distance(mcg.geom,(st_dump(msp.geom)).geom) as dist
	from med_centers_gmaps_2023 mcg , gis_tereza.metro_spirki_point msp ;

--намиране на най-близката аптека до всеки медицински център
--	drop view  gis_tereza.closest_pharmacy_to_medcntr;
drop table gis_tereza.closest_pharmacy_to_medcnt_table;
create table gis_tereza.closest_pharmacy_to_medcnt_table as
select *
from (
	select
		mcg.id as medcentr_id, mcg.geom as medcentr_geom, mcg.title as medcntr_title,
		ps.id as pharmacy_id, ps.geom as pharmacie_geom, ps.sub_title as pharmacy_title,
		st_distance(mcg.geom, ps.geom) as distance_in_meters,
		rank() over (
		   partition by mcg.id  
		   order by mcg.geom <-> ps.geom asc ) rnk
	from
		med_centers_gmaps_2023 mcg, 
		gis_tereza.pharmacies_sofia ps ) inr where inr.rnk = 1;
	
	
--създаване на таблца, която съдържа информация само за аптеките, които са най-близки до медицински центрове
drop table gis_tereza.parmacies_close_to_med_centers;
create table gis_tereza.parmacies_close_to_med_centers as
select distinct cptmt.pharmacy_id as pharmacy_id, cptmt.pharmacie_geom as pharmacy_geom, cptmt.pharmacy_title as pharmacy_title
from gis_tereza.pharmacies_sofia ps  , gis_tereza.closest_pharmacy_to_medcnt_table cptmt 
where ps.id=cptmt.pharmacy_id 
order by cptmt.pharmacy_id ;
		
--намиране на броя на медицинските центрове, за които дадена аптека е най-близка. 
-- След направения анализ за най-близките аптеки до всеки медицински център, можем да намерим броя на медицинските центрове, за които дадена аптека е най-близка.
-- Това става като преброим колко пъти се среща всяко id на аптека във вече направената таблица  closest_pharmacy_to_medcnt_table cptmt
select count(*), cptmt.pharmacy_id 
from gis_tereza.closest_pharmacy_to_medcnt_table cptmt
group by cptmt.pharmacy_id 
order by cptmt .pharmacy_id ;
 

--id- то на всеки медицински център ще се среща точно по веднъж
select count(*), cptmt.medcentr_id 
from gis_tereza.closest_pharmacy_to_medcnt_table cptmt
group by cptmt.medcentr_id
order by cptmt.medcentr_id;

--идентифициране на градоустройсвените единици без метро
drop table gis_tereza.ge_without_metrostation;
create table  gis_tereza.ge_without_metrostation as
select ges.id as ge_id, ges.regname as ge_name, ges.geom as ge_geom
from public.gradoystroistveni_edinici_2021_sofpl ges 
where not exists (
	select 1 from gis_tereza.metro_spirki_point msp  --permission denied for table metro_spirki_point 
	where st_within (msp.geom, ges.geom)
);

--идентифициране на градоустойствени единици без медицински центрове
drop table gis_tereza.ge_without_med_centers;
create table gis_tereza.ge_without_med_centers as
select ges.id as ge_id, ges.regname as ge_name, ges.geom as ge_geom
from public.gradoystroistveni_edinici_2021_sofpl ges 
where not exists(
	select 1 from med_centers_gmaps_2023 mcg2 
	where st_within(mcg2.geom, ges.geom)
);

select * from gradoystroistveni_edinici_2021_sofpl ge
	
--идентифициране на градоустройствени единици без медицински центрове и метро
drop table gis_tereza.ge_without_med_centers_and_metro ;
create table gis_tereza.ge_without_med_centers_and_metro as
select ges.id as ge_id, ges.regname as ge_name, ges.geom as ge_geom
from public.gradoystroistveni_edinici_2021_sofpl ges 
where not exists(
	select 1 from med_centers_gmaps_2023 mcg2 
	where st_within(mcg2.geom, ges.geom)
) and not exists (
	select 1 from gis_tereza.metro_spirki_point msp  
	where st_within (msp.geom, ges.geom)
)


drop table  gis_tereza.TestTableToProject;
create table gis_tereza.TestTableToProject as
select
		--mcg.id as medcentr_id, mcg.geom as medcentr_geom, mcg.title as medcntr_title,
	ps.id as pharmacy_id, ps.geom as pharmacie_geom, ps.sub_title as pharmacy_title,
	st_distance(mcg.geom, ps.geom) as distance_in_meters,
	rank() over (
	   partition by mcg.id  
	   order by mcg.geom <-> ps.geom asc ) rnk
from
	med_centers_gmaps_2023 mcg, 
	gis_tereza.pharmacies_sofia ps ;


--намиране на медицинските центрове, които попадат в буфера на всяка метростанция
drop table gis_tereza.ClosetMedCentersToMetrostationBuffer ;
create table gis_tereza.Closest_MedCenters_To_Metrostation_Buffer as
select mcg.geom as medcenter_geom, mcg.id as medcenter_id
from gis_tereza."Closet_metrostation_to_medcemters_500m" cmtmm , med_centers_gmaps_2023 mcg 
where st_within(mcg.geom, cmtmm.geom)

--намиране на аптеките, които попадат в буферпа на всяка метростанция. Защото това ще са най-близките аптеки до мед.центровете попадищи в тацзи област
create table gis_tereza.Closet_pharmacy_to_metrostation_buffer as
select pg.geom as pharmacy_geom, pg.id as pramacy_id 
from gis_tereza."Closet_metrostation_to_medcemters_500m" cmtmm, pharmacies_gmaps_2023 pg 
where st_within(pg.geom, cmtmm.geom)

--multipoint ot point metro_spirki_point
alter table gis_tereza.metro_spirki_point 
alter column geom type GEOMETRY(POINT,7801)
	using st_setSRID(St_geometryN(geom, 1),7801);
	
