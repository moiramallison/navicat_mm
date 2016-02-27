select cancel_age, paid_through_age, count(*) from yoga_sfss
where sub_60_day = 0 AND
      churn_30_day =0 AND
      cancel_date is not null
group by cancel_age, paid_through_age
order by cancel_age, paid_through_age;

select * from yoga_sfss
where sub_60_day = 0 AND
      churn_30_day =0;


select case_1, case_2, case_3, count(1)
from
(  select gcsi_user_id,
    case when cancel_age >  64 and cancel_age <= 94 then 1 
         else 0 
    end case_1,
    case when cancel_age >  64 and cancel_age <= 94 then 0 
          when  paid_through_age > 64 and paid_through_age  <= 94 and cancel_age is not null then 1
            else 0
       end   case_2,
    case when cancel_age >  64 and cancel_age <= 94 then 0 
          when   paid_through_age > 64 and paid_through_age  <= 94 and cancel_age is not null then 0
         when  cancel_age between 178 and 182 AND
                 long_hold = 1 then 1 
            else 0
       end   case_3
from st_sub_master_30_60_90) foo
group by case_1, case_2, case_3;

select churn_30_day, churn_60_day, churn_90_day, sum(sub_30_day), sum(sub_60_day), sum(sub_90_day)
from st_sub_master_30_60_90
group by churn_30_day, churn_60_day, churn_90_day;


