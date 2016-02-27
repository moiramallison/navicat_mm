create or replace view users_cd_tmp as 
 SELECT ud.drupal_user_id,
    ud.gcsi_user_id,
    ud.user_start_date AS date,
    ud.user_end_date,
    ud.subscription_id,
    ud.subscription_start_date AS start_date,
    ud.paid_through_date,
    ud.next_review_date,
    ud.cancel_date,
    ud.subscription_end_date AS end_date,
    ud.status,
    ud.entitled,
    ud.plan_id,
    ud.cid_channel channel,
    ud.source_name,
    ud.service_name,
        CASE
            WHEN (ud.winback = 'Winback'::text) THEN 1
            ELSE 0
        END AS winback,
    ud.onboarding_parent AS onboarding_segment,
    ud.subscription_cohort,
        CASE
            WHEN (ud.subscription_cohort = 'Cosmic Disclosure'::text) THEN 1
            ELSE 0
        END AS cosmic_disclosure
   FROM common.user_d ud
  WHERE ((ud.subscription_start_date >= '2015-07-01'::date) AND (ud.current_record = 'Y'::text));;