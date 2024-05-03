--------------------------------------------SR_NO	TC_ID	TEST_QUERY
		
--------------------------------------------		
TC01 and TC04		
		
select 		
(select count(*) from dev.silver_tp.event_itm_grp) as silver_count,		
(select count(*) from dev.gold_tp.event_itm_grp) as gold_count,		
(select count(*) from dev.silver_tp.event_itm_grp where hvr_change_op in (0, 4)) as rows_dropped_in_gold_for_opVal;		
 		
--------------------------------------------		
TC02		
		
select * from dev.silver_tp.event_itm_grp where eig_itm_grp_nbr=10858776 limit 1		
select * from dev.gold_tp.event_itm_grp limit 1;		
		
select msigMasterIgKey, count(*)		
from dev.gold_tp.event_itm_grp		
group by msigMasterIgKey 		
order by count(*) desc;		
		
--------------------------------------------		
TC03		
		
select		
(select count(column_name)		
from INFORMATION_SCHEMA.COLUMNS 		
where table_schema = 'silver_tp' and table_name = 'event_itm_grp'		
) as silver_count,		
(select count(column_name)		
from INFORMATION_SCHEMA.COLUMNS 		
where table_schema = 'gold_tp' and table_name = 'event_itm_grp'		
)as gold_count		
		
 		
--------------------------------------------		
TC05 DATAtype CHECK		
 		
select column_name, data_type		
from INFORMATION_SCHEMA.COLUMNS 		
where table_schema = 'gold_tp' and table_name = 'event_itm_grp';		
 		
--------------------------------------------		
TC06 Check Concatination		
		
WITH		
p1 AS (		
  SELECT eigItmGrpNbr, srcSysId as result		
  FROM dev.gold_tp.event_itm_grp		
),		
p2 AS (		
  SELECT eig_itm_grp_nbr, eig_itm_grp_nbr as expected_result		
  FROM dev.silver_tp.event_itm_grp		
  WHERE hvr_change_op not IN (0, 4)		
)		
SELECT eig_itm_grp_nbr, eigItmGrpNbr, result, expected_result		
FROM p1		
left JOIN p2 ON eigItmGrpNbr = eig_itm_grp_nbr		
		
--------------------------------------------		
TC08, TC09		
 		
select srcSysTableName, srcSysName from gold_tp.event_itm_grp		
 		
--------------------------------------------		
TC10, TC11, TC12, TC13		
 		
select distinct(opVal)		
from gold_tp.event_itm_grp		
 		
--------------------------------------------		
TC14		
select		
(select count(distinct(srcSysId)) from gold_tp.event_itm_grp) as Primary_key_count,		
(select count(*) from gold_tp.event_itm_grp) as total_count		
 		
--------------------------------------------		
TC15		
with		
ps as (		
select srcSysId, count(*) as cnt		
from gold_tp.event_itm_grp		
group by srcSysId		
order by count(*) desc		
)		
select * from ps where cnt>1		
--------------------------------------------		
TC01 and TC04
 
select 
(select count(*) from dev.silver_tp.master_itm_grp) as silver_count,
(select count(*) from dev.gold_tp.master_itm_grp) as gold_count,
(select count(*) from dev.silver_tp.master_itm_grp where hvr_change_op in (0, 4))
 
--------------------------------------------
TC03

select
(select count(column_name)
from INFORMATION_SCHEMA.COLUMNS 
where table_schema = 'gold_tp' and table_name = 'master_itm_grp'
)as gold_count,
(select count(column_name)
from INFORMATION_SCHEMA.COLUMNS 
where table_schema = 'silver_tp' and table_name = 'master_itm_grp'
) as silver_count
 
--------------------------------------------
TC05 DATAtype CHECK
 
select column_name, data_type
from INFORMATION_SCHEMA.COLUMNS 
where table_schema = 'gold_tp' and table_name = 'master_itm_grp';
 
--------------------------------------------
TC06 Check Concatination
 
select
(select srcSysId from master_itm_grp) as gold_srcSysId,
(select [as per mapping doc] 
from master_itm_grp where hvr_change_op in (0, 4)) as expected_srcSysId
 
--------------------------------------------
TC06, TC07, TC08, TC09
 
select * from gold_tp.master_itm_grp
 
(and then highlight the created columns)
 
--------------------------------------------
TC10, TC11, TC12, TC13
 
select distinct(opVal)
from gold_tp.master_itm_grp
 
--------------------------------------------
TC14
 
select count(primary_key)
from gold_tp.master_itm_grp
 
--------------------------------------------
TC15
 
select [pri_prod_key], count(*) 
from silver_tp.master_itm_grp
group by [pri_prod_key,pri_mkr_loc_key,pri_pr_nbr]
having count(*) > 1
order by count(*) desc
--------------------------------------------

TC-16
--load data from s3 directly using sql query
select * from read_files('s3://file-location/') limit 100;
select * from  
read_files('s3://unfi-data-landing-dev-s3/dev/hvr/ucs/cid94d_tp/banner/20240412/banner.20240412.145726.parquet') limit 100;




