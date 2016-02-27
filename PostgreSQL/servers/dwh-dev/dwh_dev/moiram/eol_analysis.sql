create view all_eol_emails as 
select mail from eol_active_ps3_customers union
select mail from eol_active_samsung_customers union
select mail from eol_active_sony_vita_customers union
select mail from eol_emails ;

select status, count(1)
from common.current_users
where drupal_user_id in 
   (select uid from drupal.users du
    join eol_emails EM 
      on du.mail = em .mail)
group by status;