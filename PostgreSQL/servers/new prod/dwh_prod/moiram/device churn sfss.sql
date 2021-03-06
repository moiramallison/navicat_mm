drop table if exists tmp.sub_master;

create table tmp.sub_master
as
   select dss.subscription_id,
          dss.gcsi_user_id,  
          ud.drupal_user_id, 
          [$ivend]::date ivend,
          ud.user_behavior_segment,
          [$ivend]::date - sd.start_date::date subscription_age
  from common.daily_status_snapshot dss 
  join common.user_dim ud on dss.gcsi_user_id = ud.gcsi_user_id
  join common.subscription_d sd on dss.subscription_id = sd.subscription_id
where dss.day_timestamp = [$ivend] 
  and dss.status = 'Active';

drop table if exists tmp.sub_targets;

create table tmp.sub_targets
as 
  select sd.subscription_id,
         sd.gcsi_user_id,
         sd.drupal_user_id,
         [$ivend]::date ivend,
         cancel_date,
         1 hard_cancel_flag
  from common.subscription_d sd
  where cancel_date >= [$dvbegin]
    and cancel_date <= [$dvend]
    and cancel_date <= paid_through_date;

drop table if exists tmp.video_views_master;

-- this is a little odd, but right now I think I want 
-- days since last video view as of the cancel date if they have one
create table tmp.video_views_master
as 
select vv.*,
      [$ivend]::date ivend    
from 
    (select drupal_user_id,
        max(roku)               roku_flag,
        max(js_player)          js_player_flag,
        max(apple_tv)           apple_tv_flag,
        max(iphone)             iphone_flag,
        max(ipad)               ipad_flag,
        max(android_tablet)     android_tablet_flag,
        max(android_phone)      android_phone_flag,
        sum(roku)               roku_cnt,
        sum(js_player)          js_player_cnt,
        sum(apple_tv)           apple_tv_cnt,
        sum(iphone)             iphone_cnt,
        sum(ipad)               ipad_cnt,
        sum(android_tablet)     android_tablet_cnt,
        sum(android_phone)      android_phone_cnt
    from    
        (select drupal_user_id,
            case when player_name = 'Roku' then 1 else 0 end                    roku,
            case when player_name = 'js Player' then 1 else 0 end               js_player,
            case when player_name = 'Apple TV 4' then 1 else 0 end              apple_tv,
            case when user_agent like '%iphone%gaiam%build%' then 1 else 0 end  iphone,
            case when user_agent like '%ipad%gaiam%build%' then 1 else 0 end    ipad,
            case when user_agent like '%androidtablet%' then 1 else 0 end       android_tablet,  
            case when user_agent like '%androidphone%' then 1 else 0 end        android_phone
        from
            (select user_id drupal_user_id,
                    lower(user_agent) user_agent,
                    player_name
             from common.qualified_video_views
             where created >= [$tsbegin] and
                   created < [$tsend]
            ) t
        )t2
    where roku + js_player + apple_tv + iphone + ipad  + android_tablet + android_phone  > 0
    group by drupal_user_id)vv;

  
delete from tmp.sub_master where ivend = [$ivend]::date;

insert into tmp.sub_master
   (select dss.subscription_id,
          dss.gcsi_user_id,  
          ud.drupal_user_id,
          [$ivend]::date ivend,
          ud.user_behavior_segment,
          [$ivend]::date - sd.start_date::date subscription_age
  from common.daily_status_snapshot dss 
  join common.user_dim ud on dss.gcsi_user_id = ud.gcsi_user_id
  join common.subscription_d sd on dss.subscription_id = sd.subscription_id
where dss.day_timestamp = [$ivend] 
  and dss.status = 'Active');

select ivend, count(1)
from tmp.sub_master
group by ivend;

delete from tmp.sub_targets where ivend = [$ivend]::date;

insert into tmp.sub_targets
  (select sd.subscription_id,
         sd.gcsi_user_id,
         sd.drupal_user_id,
         [$ivend]::date ivend,
         cancel_date,
         1 hard_cancel_flag
  from common.subscription_d sd
  where cancel_date >= [$dvbegin]
    and cancel_date <= [$dvend]
    and cancel_date <= paid_through_date);

select ivend, count(1)
from tmp.sub_targets
group by ivend;

delete from tmp.video_views_master where ivend = [$ivend]::date;


insert into tmp.video_views_master
(select vv.*,
      [$ivend]::date ivend    
from 
    (select drupal_user_id,
        max(roku)                             roku_flag,
        max(js_player)                        js_player_flag,
        max(iphone)                           iphone_flag,
        max(ipad)                             ipad_flag,
        max(android_tablet)                   android_tablet_flag,
        max(android_phone)                    android_phone_flag,
        round(sum(roku_watched/3600),5)       roku_watched,
        round(sum(js_player/3600),5)          js_player_watched,
        round(sum(iphone/3600),5)             iphone_watched,
        round(sum(ipad/3600),5)               ipad_watched,
        round(sum(android_tablet/3600),5)     android_tablet_watched,
        round(sum(android_phone/3600),5)      android_phone_watched,
        max(eng_roku)                         eng_roku_flag,
        max(eng_js_player)                    eng_js_player_flag,
        max(eng_iphone)                       eng_iphone_flag,
        max(eng_ipad)                         eng_ipad_flag,
        max(eng_android_tablet)               eng_android_tablet_flag,
        max(eng_android_phone)                eng_android_phone_flag,
        round(sum(eng_roku_watched/3600),5)   eng_roku_watched,
        round(sum(eng_js_player/3600),5)      eng_js_player_watched,
        round(sum(eng_iphone/3600),5)         eng_iphone_watched,
        round(sum(eng_ipad/3600),5)           eng_ipad_watched,
        round(sum(eng_android_tablet/3600),5) eng_android_tablet_watched,
        round(sum(eng_android_phone/3600),5)  eng_android_phone_watched
    from   
        (select t2.*,
            case when engaged_view = 1 then roku else 0 end                     eng_roku,
            case when engaged_view = 1 then roku_watched else 0 end             eng_roku_watched,
            case when engaged_view = 1 then js_player else 0 end                eng_js_player,
            case when engaged_view = 1 then js_player_watched else 0 end        eng_js_player_watched,
            case when engaged_view = 1 then iphone else 0 end                   eng_iphone,
            case when engaged_view = 1 then iphone_watched else 0 end           eng_iphone_watched,
            case when engaged_view = 1 then ipad else 0 end                     eng_ipad,
            case when engaged_view = 1 then ipad_watched else 0 end             eng_ipad_watched,
            case when engaged_view = 1 then android_tablet else 0 end           eng_android_tablet,
            case when engaged_view = 1 then android_tablet_watched else 0 end   eng_android_tablet_watched,
            case when engaged_view = 1 then android_phone else 0 end            eng_android_phone,
            case when engaged_view = 1 then android_phone_watched else 0 end    eng_android_phone_watched
        from
            (select drupal_user_id,
                engaged_view,
                case when player_name = 'Roku' then 1 else 0 end                            roku,
                case when player_name = 'js Player' then 1 else 0 end                       js_player,
                case when user_agent like '%iphone%gaiam%build%' then 1 else 0 end          iphone,
                case when user_agent like '%ipad%gaiam%build%' then 1 else 0 end            ipad,
                case when user_agent like '%androidtablet%' then 1 else 0 end               android_tablet,  
                case when user_agent like '%androidphone%' then 1 else 0 end                android_phone,
                case when player_name = 'Roku' then watched else 0 end                      roku_watched,
                case when player_name = 'js Player' then watched else 0 end                 js_player_watched,
                case when user_agent like '%iphone%gaiam%build%' then watched else 0 end    iphone_watched,
                case when user_agent like '%ipad%gaiam%build%' then watched else 0 end      ipad_watched,
                case when user_agent like '%androidtablet%' then watched else 0 end         android_tablet_watched,  
                case when user_agent like '%androidphone%' then watched else 0 end          android_phone_watched
            from
                (select user_id drupal_user_id,
                        watched,
                        engaged_view,
                        lower(user_agent) user_agent,
                        player_name
                 from common.qualified_video_views
                 where created >= [$tsbegin] and
                       created < [$tsend]
                ) t
            )t2
        ) t3
    where roku + js_player + iphone + ipad  + android_tablet + android_phone  > 0
    group by drupal_user_id) vv );
  
delete from tmp.sfss where ivend = [$ivend]::date;

insert into tmp.sfss
(select sm .*,
        roku_flag,
        js_player_flag,
        iphone_flag,
        ipad_flag,
        android_tablet_flag,
        android_phone_flag,
        roku_watched,
        js_player_watched,
        iphone_watched,
        ipad_watched,
        android_tablet_watched,
        android_phone_watched,
        eng_roku_flag,
        eng_js_player_flag,
        eng_iphone_flag,
        eng_ipad_flag,
        eng_android_tablet_flag,
        eng_android_phone_flag,
        eng_roku_watched,
        eng_js_player_watched,
        eng_iphone_watched,
        eng_ipad_watched,
        eng_android_tablet_watched,
        eng_android_phone_watched,
        coalesce(st.hard_cancel_flag,0) churn_flag
 from tmp.sub_master sm 
 join tmp.video_views_master vv
    on sm.drupal_user_id = vv.drupal_user_id and 
       sm.ivend = vv.ivend
 left join tmp.sub_targets st 
    on sm.drupal_user_id = st.drupal_user_id and 
       sm.ivend = st.ivend);
/*
create table tmp.device_group_lookup
 (group_number int2,
  description text);

insert into tmp.device_group_lookup values(2,'Web Only');
insert into tmp.device_group_lookup values(1,'Roku Only');
insert into tmp.device_group_lookup values(10,'Web + iPad');
insert into tmp.device_group_lookup values(6,'Web + iPhone');
insert into tmp.device_group_lookup values(3,'Web + Roku');
insert into tmp.device_group_lookup values(8,'iPad Only');
insert into tmp.device_group_lookup values(4,'iPhone Only');
insert into tmp.device_group_lookup values(14,'Web, iPad, iPhone');
insert into tmp.device_group_lookup values(12,'iPad + iPhone');
insert into tmp.device_group_lookup values(9,'Roku + iPad');
insert into tmp.device_group_lookup values(7,'Roku, Web, iPhone');
insert into tmp.device_group_lookup values(5,'Roku + iPhone');
insert into tmp.device_group_lookup values(11,'Roku, Web, iPad');
insert into tmp.device_group_lookup values(18,'Web, Android Tablet');
insert into tmp.device_group_lookup values(15,'Roku, Web, iPad, iPhone');
insert into tmp.device_group_lookup values(16,'Android Tablet Only');
insert into tmp.device_group_lookup values(13,'Roku,  iPad, iPhone');
*/


