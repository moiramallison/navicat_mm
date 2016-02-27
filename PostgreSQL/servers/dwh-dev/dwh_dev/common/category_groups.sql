drop table if exists tmp.categories_tmp;

create table tmp.categories_tmp as
(select a4.nid,
    case num_categories
        when 1 then t1
        when 2 then t1 || ',' ||  t2
        when 3 then t1 || ',' ||  t2 ||  ',' ||  t3
        when 4 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4
        when 5 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5
        when 6 then t1 || ',' ||  t2 ||  ',' ||  t3 ||  ',' ||  t4 ||  ',' ||  t5 ||  ',' ||  t6
    end categories
from      
    (select a3.nid,
        max(t1) t1,
        max(t2) t2,
        max(t3) t3,
        max(t4) t4,
        max(t5) t5,
        max(t6) t6,
        num_categories
    from
        (select a2.nid, 
           case when rn = 1 then category else null end t1,
           case when rn = 2 then category else null end t2,
           case when rn = 3 then category else null end t3,
           case when rn = 4 then category else null end t4,
           case when rn = 5 then category else null end t5,
           case when rn = 6 then category else null end t6,
           num_categories
        from 
            (select a1.*,
               last_value(rn) over (partition by nid) num_categories
             from
               (SELECT n.nid, td.name as category,
                    row_number() over (partition by n.nid order by td.name) rn
                FROM drupal.node n
                join drupal.term_node tn on n.nid = tn.nid
                JOIN drupal.term_data td ON tn.tid = td.tid
                WHERE td.vid = 1) a1
              ) a2
          ) a3
    group by a3.nid, num_categories) a4);

drop table if exists tmp.groups_tmp;

create table tmp.groups_tmp
(gid serial,
 category_group varchar(255));

insert into tmp.groups_tmp (category_group)
(select distinct categories from tmp.categories_tmp);


drop table if exists tmp.nid_categories;

create table tmp.nid_categories as
(select tt.nid, tg.gid
 from tmp.categories_tmp tt
 join tmp.groups_tmp tg
    on tt.categories = tg.category_group);

drop table if exists common.category_groups;

create table common.category_groups
as (select gid, 
       td.tid,
       td.name,
       case WHEN strpos(g.category_group, td.name) = 1
            THEN 'Primary'
            ELSE 'Not Primary'
       end primary_category
    from tmp.groups_tmp g
    join 
      (SELECT d.tid,  d.name
       FROM drupal.term_data d 
       WHERE d.vid = 1) td
    on strpos(g.category_group, td.name) > 0
)
;
    