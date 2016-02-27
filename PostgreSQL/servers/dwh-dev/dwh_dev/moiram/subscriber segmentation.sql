select dd.month_text, dd.year_text, ct.campaign, channel, winback, 
       ud.user_behavior_segment, count(distinct gcsi_user_id)
from common.user_d ud
join common.date_d dd on ud.subscription_start_date::date = dd.day_timestamp::date 
left join 
  (select  reported_channel, campaign, left(campaign,pos) channel from 
    (select ct.reported_channel, ct.campaign,position('|' in ct.campaign)-2 pos
      from common.campaign_tracking ct) as foo) ct
    on ud.cid_channel = ct.reported_channel
 where month_key >= 201410 
   and month_key <= 201510
group by  dd.month_text, dd.year_text, ct.campaign, channel, winback,
       ud.user_behavior_segment ;