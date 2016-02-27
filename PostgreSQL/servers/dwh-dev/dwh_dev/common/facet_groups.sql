drop table if exists tmp.facets_tmp;

create table  tmp.facets_tmp
(vid                        int,
 nid                        int,
 facet_type         varchar(10),
 facet_name     varchar(20),
 tid            int);


insert into tmp.facets_tmp
select vid, nid,
       'yoga',
       'level',
       field_facet_yoga_level_value
from drupal.content_field_facet_yoga_level
where field_facet_yoga_level_value is not null;

insert into tmp.facets_tmp
select vid, nid,
       'yoga',
       'focus',
       field_facet_yoga_focus_value
from drupal.content_field_facet_yoga_focus
where field_facet_yoga_focus_value is not null;

insert into tmp.facets_tmp
select vid, nid,
       'yoga',
       'style',
       field_facet_yoga_style_value
from drupal.content_field_facet_yoga_style
where field_facet_yoga_style_value is not null;

insert into tmp.facets_tmp
select vid, nid,
       'yoga',
       'teacher',
       field_facet_yoga_teacher_value
from drupal.content_field_facet_yoga_teacher
where field_facet_yoga_teacher_value is not null;

-- don't insert fitness row if yoga row has same value
insert into tmp.facets_tmp
select vid, nid,
       'fitness',
       'level',
       field_facet_fitness_level_value
from drupal.content_field_facet_fitness_level cf
where field_facet_fitness_level_value is not null
  and not exists
      (select vid 
       from tmp.facets_tmp f
       where cf.vid = f.vid
         and cf.nid = f.nid
         and f.facet_name = 'level' 
         and cf.field_facet_fitness_level_value = f.tid);

insert into tmp.facets_tmp
select vid, nid,
       'fitness',
       'focus',
       field_facet_fitness_speciality_value
from drupal.content_field_facet_fitness_speciality cf
where field_facet_fitness_speciality_value is not null
and not exists
      (select vid 
       from tmp.facets_tmp f
       where cf.vid = f.vid
         and cf.nid = f.nid
         and f.facet_name = 'focus' 
         and cf.field_facet_fitness_speciality_value = f.tid);


insert into tmp.facets_tmp
select vid, nid,
       'fitness',
       'style',
       field_facet_fitness_style_value
from drupal.content_field_facet_fitness_style cf 
where field_facet_fitness_style_value is not null
and not exists
      (select vid 
       from tmp.facets_tmp f
       where cf.vid = f.vid
         and cf.nid = f.nid
         and f.facet_name = 'style' 
         and cf.field_facet_fitness_style_value = f.tid);


insert into tmp.facets_tmp
select vid, nid,
       'fitness',
       'teacher',
       field_facet_fitness_teacher_value
from drupal.content_type_product_video  tpv
where field_facet_fitness_teacher_value is not null
and not exists
      (select vid 
       from tmp.facets_tmp f
       where tpv.vid = f.vid
         and tpv.nid = f.nid
         and f.facet_name = 'teacher' 
         and tpv.field_facet_fitness_teacher_value = f.tid);

insert into tmp.facets_tmp
select vid, nid,
       'interview',
       'guest',
       field_facet_interview_guest_value
from drupal.content_field_facet_interview_guest
where field_facet_interview_guest_value is not null;

insert into tmp.facets_tmp
select vid, nid,
       'interview',
       'host',
       field_facet_interview_host_value
from drupal.content_field_facet_interview_host
where field_facet_interview_host_value is not null;

alter table tmp.facets_tmp add name varchar(255);

update tmp.facets_tmp f
set name = td.name
from 
   (select * from drupal.term_data where vid in (36,41)) td
where f.tid = td.tid;

drop table if exists tmp.facet_grp_tmp;

create table tmp.facet_grp_tmp as
(select a4.nid, facet_name,
    case num_facets
        when 1 then t1
        when 2 then t1 || ',' ||  t2
        when 3 then t1 || ',' ||  t2 ||  ',' ||  t3
        when 4 then t1 || ',' ||  t2 ||  ',' ||  t3  ||',' ||  t4 
        when 5 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5
        when 6 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6
        when 7 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6 ||  ',' ||  t7 
        when 8 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6 ||  ',' ||  t7 ||  ',' ||  t8 
     end facets
from      
    (select a3.nid,  facet_name,
        max(t1) t1,
        max(t2) t2,
        max(t3) t3,
        max(t4) t4,
        max(t5) t5,
        max(t6) t6,
        max(t7) t7,
        max(t8) t8,
        num_facets
    from
        (select a2.nid, facet_name,
           case when rn = 1 then name else null end t1,
           case when rn = 2 then name else null end t2,
           case when rn = 3 then name else null end t3,
           case when rn = 4 then name else null end t4,
           case when rn = 5 then name else null end t5,
           case when rn = 6 then name else null end t6,
           case when rn = 7 then name else null end t7,
           case when rn = 8 then name else null end t8,
           num_facets
        from 
            (select a1.*,
               last_value(rn) over (partition by nid, facet_name) num_facets
             from
               (SELECT f.*,
                    row_number() over (partition by nid,  facet_name order by name) rn
                FROM tmp.facets_tmp f) a1
              ) a2
          ) a3
    group by a3.nid, facet_name, num_facets) a4);

drop table if exists tmp.groups_tmp;

create table tmp.groups_tmp
(gid serial,
facet_group varchar(255));

insert into tmp.groups_tmp (facet_group)
    (select distinct facets from tmp.facet_grp_tmp);

drop table if exists common.facet_groups;

create table common.facet_groups
as (select gid, 
    td.tid,
    td.name,
    case WHEN strpos(g.facet_group, td.name) = 1
        THEN 'Primary'
        ELSE 'Not Primary'
    end primary_facet
from tmp.groups_tmp g
join 
    (SELECT d.tid,  d.name
     FROM drupal.term_data d 
     WHERE d.vid  in (36,41)) td
    on strpos(g.facet_group, td.name) > 0);

--translate the single_valued facets
drop table if exists tmp.sv_facet_tmp;

create table tmp.sv_facet_tmp as
(select nid, 
   coalesce(td1.name, td2.name) facet_duration,
   td3.name facet_interview_collection,
   td4.name facet_interview_topic
from common.v_facets vf
left join drupal.term_data td1
  on vf.field_facet_yoga_duration_value = td1.tid and
     td1.vid = 36
left join drupal.term_data td2
  on vf.field_facet_fitness_duration_value = td2.tid and
     td2.vid = 36
left join drupal.term_data td3
  on vf.field_facet_interview_collection_value = td3.tid and
     td3.vid = 36
left join drupal.term_data td4
  on vf.field_facet_interview_topic_value = td4.tid and
     td4.vid = 36);

drop table if exists tmp.nid_facets;

create table tmp.nid_facets as
(select n3.*,
        facet_duration,
        facet_interview_collection,
        facet_interview_topic
from 
    (select nid,
            max(facet_focus) facet_focus,
            max(facet_style) facet_style,
            max(facet_level) facet_level,
            max(facet_teacher) facet_teacher,
            max(facet_guest) facet_guest,
            max(facet_host) facet_host
     from
        (select nid,
                case when facet_name = 'focus' then gid else null end facet_focus,
                case when facet_name = 'style' then gid else null end facet_style,
                case when facet_name = 'level' then gid else null end facet_level,
                case when facet_name = 'teacher' then gid else null end facet_teacher,
                case when facet_name = 'guest' then gid else null end facet_guest,
                case when facet_name = 'host' then gid else null end facet_host
            from
                (select f.nid,  f.facet_name, tg.gid
            from tmp.facet_grp_tmp f
            join tmp.groups_tmp tg
              on f.facets = tg.facet_group) n1
          )n2
      group by nid)n3
 left join tmp.sv_facet_tmp sv
     on n3.nid = sv.nid);
     
     -- just in case a nid only as sv facets...
 insert into tmp.nid_facets(nid,facet_duration,
        facet_interview_collection,
        facet_interview_topic)
 select * from tmp.sv_facet_tmp sv
 where sv.nid not in 
    (select nid from tmp.nid_facets);

drop table if exists common.facet_levels;
-- I may well end up doing this for more facets...
create table common.facet_levels as 
(select gid, 
        max(level_1) level_1,
        max(level_2) level_2,
        max(level_3) level_3
from
(select gid, 
    case when "name"  like '%1%' then 1 else 0 end level_1,
    case when "name"  like '%2%' then 1 else 0 end level_2,
    case when "name"  like '%3%' then 1 else 0 end level_3
 from common.facet_groups
 where gid in 
   (select distinct facet_level from tmp.nid_facets)) t
group by gid);
