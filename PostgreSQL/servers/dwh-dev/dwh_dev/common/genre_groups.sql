drop table if exists tmp.genres_tmp;

create table tmp.genres_tmp as
(select a4.nid,
    case num_genres
        when 1 then t1
        when 2 then t1 || ',' ||  t2
        when 3 then t1 || ',' ||  t2 ||  ',' ||  t3
    end genres
from      
    (select a3.nid,
        max(t1) t1,
        max(t2) t2,
        max(t3) t3,
        num_genres
    from
        (select a2.nid, 
           case when rn = 1 then genre else null end t1,
           case when rn = 2 then genre else null end t2,
           case when rn = 3 then genre else null end t3,
           num_genres
        from 
            (select a1.*,
               last_value(rn) over (partition by nid) num_genres
             from
               (SELECT tn.nid, td.name as genre,
                    row_number() over (partition by tn.nid order by td.name desc) rn
                FROM drupal.term_node tn
                INNER JOIN drupal.term_data td ON tn.tid = td.tid
                WHERE td.vid = 7) a1
              ) a2
          ) a3
    group by a3.nid, num_genres) a4);

drop table if exists tmp.groups_tmp;

create table tmp.groups_tmp
(gid serial,
 genre_group varchar(255));

insert into  tmp.groups_tmp (genre_group)
(select distinct genres from  tmp.genres_tmp);


drop table if exists tmp.nid_genres;

create table  tmp.nid_genres as
(select tt.nid, tg.gid
 from tmp.genres_tmp tt
 join tmp.groups_tmp tg
    on tt.genres = tg.genre_group);

drop table if exists common.genre_groups;

create table common.genre_groups
as (select gid, 
       td.tid,
       td.name,
       case WHEN strpos(g.genre_group, td.name) = 1
            THEN 'Primary'
            ELSE 'Not Primary'
       end primary_genre
    from tmp.groups_tmp g
    join 
        (SELECT d.tid,  d.name
        FROM drupal.term_data d 
        WHERE d.vid = 7) td
    on strpos(g.genre_group, td.name) > 0
);


