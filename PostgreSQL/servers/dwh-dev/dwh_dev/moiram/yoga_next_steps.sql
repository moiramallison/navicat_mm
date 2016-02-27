select churn_30_day, count(1) from yoga_sfss
group by churn_30_day;


select any_st_engagement, churn, count(1)
from 
(select case when  greatest(st_engagement_ratio_1, st_engagement_ratio_2, st_engagement_ratio_3) > 0 then 1 
             else 0 end any_st_engagement,
       greatest(churn_30_day, churn_60_day, churn_90_day) churn
from yoga_sfss) foo
group by any_st_engagement, churn;


select guide_title,  churn_30_day, count(distinct gcsi_user_id) c
from (select distinct guide_title, gcsi_user_id, churn_30_day
from yoga_sfss y
join common.video_daily_cube vdc
    on y.drupal_user_id = vdc.user_id
join common.guide_d g
    on vdc.media_nid = g.media_nid
where 
    y.sub_30_day = 1 and
    y.num_guide_opt_ins_1 > 0)foo
group by guide_title,  churn_30_day

select * from tmp.guides;

select num_guide_opt_ins_3 + num_guide_opt_ins_2 + num_guide_opt_ins_1, count(1), sum(churn_90_day)
from yoga_sfss where sub_90_day =1 and (num_guide_opt_ins_3 + num_guide_opt_ins_2 +num_guide_opt_ins_1) > 0
group by num_guide_opt_ins_3 + num_guide_opt_ins_2 + num_guide_opt_ins_1
order by num_guide_opt_ins_3 + num_guide_opt_ins_2 + num_guide_opt_ins_1;

select num_guide_opt_ins_1, count(1), sum(churn_30_day)
from yoga_sfss where sub_30_day =1 and num_guide_opt_ins_1 > 0
group by num_guide_opt_ins_1
order by num_guide_opt_ins_1;
       