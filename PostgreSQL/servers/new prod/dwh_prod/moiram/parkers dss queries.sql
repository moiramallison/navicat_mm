
    SELECT
    count(subscription_id) as total
    FROM (
    SELECT
    distinct on (dss.subscription_id)
      dss.subscription_id
      , dss.status
      FROM common.daily_status_snapshot dss
      INNER JOIN common.subscription_d sd ON sd.subscription_id = dss.subscription_id
      INNER JOIN common.user_dim ud ON ud.gcsi_user_id = dss.gcsi_user_id
      WHERE
      (
        dss.day_timestamp >= '20160425'
        AND dss.day_timestamp < '20160426'
        AND dss.paid_through_date >= '20140424'::date + INTERVAL '6 hours'
        AND dss.paid_through_date < '20160425'::date + INTERVAL '6 hours'
        AND dss.status NOT IN ('Suspended', 'Trial Cancelled', 'Trial Hold', 'Trial')
         AND ud.user_behavior_segment = 'Seeking Truth' AND (('20140424'::date - sd.start_date::date)) >= 365
      )
      OR
            (
        dss.day_timestamp >= '20160425'
        AND dss.day_timestamp < '20160426'
        AND    dss.paid_through_date::date = '20140424'::date - INTERVAL '15 day'
        AND dss.status = 'Suspended'
         AND ud.user_behavior_segment = 'Seeking Truth' AND (('20140424'::date - sd.start_date::date)) >= 365
      )

    )t;


drop table if exists moiram.subs_20160424_muc_old;
--actives
create table moiram.subs_20160424_muc_old as 
    SELECT
      distinct dss.subscription_id
--    count(distinct dss.subscription_id) as total
    FROM common.daily_status_snapshot dss
    INNER JOIN common.subscription_d sd ON dss.subscription_id = sd.subscription_id
    INNER JOIN common.user_dim ud ON ud.gcsi_user_id = dss.gcsi_user_id
    WHERE
    ((
      dss.day_timestamp >= '20160424'
      AND dss.day_timestamp < '20160425'
      AND dss.paid_through_date >= '20160423'::date + INTERVAL '6 hours'
      AND dss.status NOT IN ('Hold', 'Trial Cancelled', 'Trial Hold', 'Trial')
		   AND dss.subscription_id IN (SELECT umf.subscription_id FROM gcsi.updater_manual_fix umf) 
    )
    OR
    (
      dss.day_timestamp >= '20160424'
      AND dss.day_timestamp < '20160425'
            AND    dss.paid_through_date >= '20160423'::date - INTERVAL '15 day'
            AND dss.status = 'Suspended'
				 AND dss.subscription_id IN (SELECT umf.subscription_id FROM gcsi.updater_manual_fix umf) 
    ));


drop table if exists moiram.subs_20160425_muc_old;
--actives
create table moiram.subs_20160425_muc_old as 
    SELECT
      distinct dss.subscription_id
--    count(distinct dss.subscription_id) as total
    FROM common.daily_status_snapshot dss
    INNER JOIN common.subscription_d sd ON dss.subscription_id = sd.subscription_id
    INNER JOIN common.user_dim ud ON ud.gcsi_user_id = dss.gcsi_user_id
    WHERE
    ((
      dss.day_timestamp >= '20160425'   --measure date
      AND dss.day_timestamp < '20160426'
      AND dss.paid_through_date >= '20160424'::date + INTERVAL '6 hours'
      AND dss.status NOT IN ('Hold', 'Trial Cancelled', 'Trial Hold', 'Trial')
		   AND dss.subscription_id IN (SELECT umf.subscription_id FROM gcsi.updater_manual_fix umf) 
    )
    OR
    (
      dss.day_timestamp >= '20160424'
      AND dss.day_timestamp < '20160425'
            AND    dss.paid_through_date >= '20160423'::date - INTERVAL '15 day'
            AND dss.status = 'Suspended'
				 AND dss.subscription_id IN (SELECT umf.subscription_id FROM gcsi.updater_manual_fix umf) 
    ));

