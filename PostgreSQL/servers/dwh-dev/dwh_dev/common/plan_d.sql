drop table if exists common.plan_d;

CREATE TABLE common.plan_d (
dwh_plan_id		serial unique,
gcsi_plan_id 	int8,
segment_id 		int8,
plan_name 		text COLLATE "default",
plan_period 	interval(6)
);


insert into common.plan_d (gcsi_plan_id, segment_id, plan_name, plan_period)
select distinct 
   plan_id, 
   segment_id,
   plan_name,
   plan_period
from common.user_d
order by plan_id, segment_id;



