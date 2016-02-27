
/*
SELECT v.name, td.name as cat_name
        FROM drupal.term_node tn
        INNER JOIN drupal.term_data td ON tn.tid = td.tid
        INNER JOIN drupal.vocabulary v ON v.vid = td.vid
        INNER JOIN drupal.term_hierarchy th ON tn.tid = th.tid
        WHERE tn.nid = 104861;
*/
drop table if exists cd_video_d_tmp;

create table  cd_video_d_tmp as
--insert into cd_video_d_tmp
select n.nid page_nid, n.vid, n.title, cv.field_feature_nid_nid, 1 feature
from drupal.node n
join drupal.content_field_feature_nid cv
  ON n.nid = cv.nid
where n.nid in 
(select tn.nid
        FROM drupal.term_node tn
        INNER JOIN drupal.term_data td ON tn.tid = td.tid
        INNER JOIN drupal.vocabulary v ON v.vid = td.vid
        INNER JOIN drupal.term_hierarchy th ON tn.tid = th.tid
        WHERE v.vid=6 and td.name = 'Cosmic Disclosure');
/*
delete from cd_video_tmp where feature = 0;

insert into cd_video_d_tmp
select n.nid page_nid, n.vid, n.title, cv.field_preview_nid_nid, 0
from drupal.node n
join drupal.content_field_preview_nid cv
  ON n.nid = cv.nid
where n.nid in 
(select tn.nid
        FROM drupal.term_node tn
        INNER JOIN drupal.term_data td ON tn.tid = td.tid
        INNER JOIN drupal.vocabulary v ON v.vid = td.vid
        INNER JOIN drupal.term_hierarchy th ON tn.tid = th.tid
        WHERE v.vid=6 and td.name = 'Cosmic Disclosure');

*/
delete from cd_video_d_tmp where field_feature_nid_nid is null;
/*select *
from drupal.content_field_feature_nid
where nid in 
(select tn.nid
        FROM drupal.term_node tn
        INNER JOIN drupal.term_data td ON tn.tid = td.tid
        INNER JOIN drupal.vocabulary v ON v.vid = td.vid
        INNER JOIN drupal.term_hierarchy th ON tn.tid = th.tid
        WHERE v.vid=6 and td.name = 'Cosmic Disclosure');
*/
