
drop table if exists common.guide_d;

create table common.guide_d as 
(select distinct on (tgd.nid)
				n.nid guide_nid,
        n.vid guide_vid,
        n.status guide_status,
        n.title guide_title,
        tgd.nid guide_day_nid,
        n2.vid guide_day_vid, 
        tgd.field_guide_day_weight_value guide_day,
        n2.title guide_day_title,
				ffn.field_feature_nid_nid media_nid,
        n3.vid media_vid,
        ss.site_segment
 from drupal.node n
 left join drupal.content_type_guide_day tgd
   on n.nid = tgd.field_guide_day_guide_nid
 left join drupal.node n2
   on tgd.nid = n2.nid
left join drupal.content_field_feature_nid ffn
   on tgd.nid = ffn.nid
 left join drupal.node n3
   on ffn.field_feature_nid_nid = n3.nid
left join 
   (select nid, site_segment  -- the coalesce results in some noise.  get rid of it
    from
			(SELECT tn.nid, coalesce(td2.name,  td.name) site_segment
        FROM drupal.term_node tn
        INNER JOIN drupal.term_data td ON tn.tid = td.tid
        INNER JOIN drupal.vocabulary v ON v.vid = td.vid
        INNER JOIN drupal.term_hierarchy th ON tn.tid = th.tid
        left join drupal.term_data td2 on th.parent = td2.tid) t
     where site_segment in (select site_segment from common.admin_category_mapping))ss
  on n.nid = ss.nid);

