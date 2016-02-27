

select drupal_user_id,  from
(select drupal_user_id,
             sum(v.num_views) num_views
      from users_cd_tmp u
      left join cd_video v
         on u.drupal_user_id = v.user_id
      where u.cosmic_disclosure = 1
      group by drupal_user_id) t
where num_views = 0 or num_views is null