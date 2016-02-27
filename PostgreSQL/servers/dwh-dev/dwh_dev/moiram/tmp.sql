drop table if exists users_cd_tmp;

create table users_cd_tmp as 
 select ud.drupal_user_id,
    ud.gcsi_user_id,
    sd.start_date AS start_date,
    sd.status,
    pd.gcsi_plan_id,
    case
       when (ud.winback = 'winback'::text) then 1
       else 0
    end as winback,
    ud.onboarding_parent AS onboarding_segment,
    case
        when cd.gcsi_user_id is not null
        then 1
        else 0
    end as cosmic_disclosure
   from common.user_dim ud
   join common.subscription_d sd 
       on ud.current_subscription = sd.subscription_id
   join 
      (select distinct gcsi_user_id 
       from common.cohort_d
       where cohort_name = 'Cosmic Disclosure') cd
       on ud.gcsi_user_id = cd.gcsi_user_id
   join common.plan_d pd
       on sd.dwh_plan_id = pd.dwh_plan_id
   where sd.start_date >= '2015-07-01'::date;