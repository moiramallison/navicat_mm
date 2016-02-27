drop table if exists tmp.tags_tmp;

create table tmp.tags_tmp as
(select a4.nid,
    case num_tags
        when 1 then t1
        when 2 then t1 || ',' ||  t2
        when 3 then t1 || ',' ||  t2 ||  ',' ||  t3
        when 4 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4
        when 5 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5
        when 6 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6
        when 7 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6 ||  ',' ||  t7 
        when 8 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6 ||  ',' ||  t7 ||  ',' ||  t8 
        when 9 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6 ||  ',' ||  t7 ||  ',' ||  t8 ||  ',' ||  t9
        when 10 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6 ||  ',' ||  t7 ||  ',' ||  t8 ||  ',' ||  t9 ||  ',' ||  t10
        when 11 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6 ||  ',' ||  t7 ||  ',' ||  t8 ||  ',' ||  t9 ||  ',' ||  t10 ||  ',' ||  t11 
        when 12 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6 ||  ',' ||  t7 ||  ',' ||  t8 ||  ',' ||  t9 ||  ',' ||  t10 ||  ',' ||  t11 ||  ',' ||  t12
    end tags
from      
    (select a3.nid,
        max(t1) t1,
        max(t2) t2,
        max(t3) t3,
        max(t4) t4,
        max(t5) t5,
        max(t6) t6,
        max(t7) t7,
        max(t8) t8,
        max(t9) t9,
        max(t10) t10,
        max(t11) t11,
        max(t12) t12,
        num_tags
    from
        (select a2.nid, 
           case when rn = 1 then tag else null end t1,
           case when rn = 2 then tag else null end t2,
           case when rn = 3 then tag else null end t3,
           case when rn = 4 then tag else null end t4,
           case when rn = 5 then tag else null end t5,
           case when rn = 6 then tag else null end t6,
           case when rn = 7 then tag else null end t7,
           case when rn = 8 then tag else null end t8,
           case when rn = 9 then tag else null end t9,
           case when rn = 10 then tag else null end t10,
           case when rn = 11 then tag else null end t11,
           case when rn = 12 then tag else null end t12,
           num_tags
        from 
            (select a1.*,
               last_value(rn) over (partition by nid) num_tags
             from
               (SELECT tn.nid,  td.name as tag,
                    row_number() over (partition by tn.nid order by td.name) rn
                FROM drupal.term_node tn
                INNER JOIN drupal.term_data td ON tn.tid = td.tid
                WHERE td.vid = 6) a1
              ) a2
          ) a3
    group by a3.nid, num_tags) a4);


drop table if exists tmp.groups_tmp;

create table tmp.groups_tmp
(gid serial,
 tag_group varchar(255));

insert into tmp.groups_tmp (tag_group)
(select distinct tags from tmp.tags_tmp);


drop table if exists tmp.nid_tags;

create table tmp.nid_tags as
(select tt.nid, tg.gid
 from tmp.tags_tmp tt
 join tmp.groups_tmp tg
    on tt.tags = tg.tag_group);

drop table if exists common.tag_groups;

create table common.tag_groups
as (select gid, 
       td.tid,
       td.name,
       case WHEN strpos(g.tag_group, td.name) = 1
            THEN 'Primary'
            ELSE 'Not Primary'
       end primary_tag
    from tmp.groups_tmp g
    join 
      (SELECT d.tid,  d.name
       FROM drupal.term_data d 
       WHERE d.vid = 6) td
    on strpos(g.tag_group, td.name) > 0
)
;
    