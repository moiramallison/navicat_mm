

create or replace view common.video_tmp as
( SELECT
        CASE
            WHEN (suh.uid = 0) THEN suh.client_uid
            ELSE suh.uid
        END AS user_id,
    suh.id,
    suh.nid,
    suh.extra_nid,
    suh.created,
    suh.position,
    suh.join_ms,
    suh.watched,
    suh.paused,
    suh.seekcount,
    suh.type
   FROM drupal.smfplayer_user_history suh
  WHERE (((suh.ign = 0) AND (suh.bad = 0)) AND (suh.watched > 15)));

create or replace view common.v_video_category as 
 SELECT tn.nid,
    td.name AS video_category
   FROM (drupal.term_node tn
     JOIN drupal.term_data td ON ((tn.tid = td.tid)))
  WHERE (td.vid = 1);

 create or replace view common.v_content_origin as 
SELECT n.vid,
    td.name AS content_origin
   FROM ((drupal.node n
     JOIN drupal.term_node tn ON ((n.nid = tn.nid)))
     JOIN drupal.term_data td ON ((tn.tid = td.tid)))
  WHERE (td.vid = 13);

 create or replace view common.v_admin_category as 
SELECT n.vid,
    td.name AS admin_category
   FROM ((drupal.node n
     JOIN drupal.term_node tn ON ((n.nid = tn.nid)))
     JOIN drupal.term_data td ON ((tn.tid = td.tid)))
  WHERE (td.vid = 12);

 create or replace view common.v_series as 
SELECT f.nid,
    f.field_series_nid_nid,
    n.title AS series_title,
    img.filepath AS series_coverart_filepath
   FROM (((drupal.node n
     JOIN drupal.content_field_series_nid f ON ((n.nid = f.field_series_nid_nid)))
     LEFT JOIN drupal.content_field_series_coverart_image cfsci ON ((n.vid = cfsci.vid)))
     LEFT JOIN drupal.files img ON ((cfsci.field_series_coverart_image_fid = img.fid)));

create or replace view common.v_node as 
 SELECT dn.nid,
    bgv.duration,
    to_timestamp((dn.created)::double precision) AS created_ts,
    dn.title
   FROM drupal.node dn
     JOIN drupal.brightcove_gtv_video bgv ON (bgv.ref_id)::text = (dn.title)::text;

create  or replace view common.v_fields as 
 SELECT n.nid,
    sn.field_season_value AS season,
    cst.field_cast_value AS "cast",
    cp.field_copyright_value AS copyright,
    dr.field_director_value AS director,
    pr.field_producer_value AS producer,
    st.field_studio_value AS studio,
    bd.field_body_value AS long_description
   FROM (((((((drupal.node n
     LEFT JOIN drupal.content_field_season sn ON ((n.vid = sn.vid)))
     LEFT JOIN drupal.content_field_cast cst ON ((n.vid = cst.vid)))
     LEFT JOIN drupal.content_field_copyright cp ON ((n.vid = cp.vid)))
     LEFT JOIN drupal.content_field_director dr ON ((n.vid = dr.vid)))
     LEFT JOIN drupal.content_field_studio st ON ((n.vid = st.vid)))
     LEFT JOIN drupal.content_field_body bd ON ((n.vid = bd.vid)))
     LEFT JOIN drupal.content_field_producer pr ON ((n.vid = pr.vid)));

create or replace view common.v_facets as
 SELECT drupal.content_type_product_video.vid,
    drupal.content_type_product_video.nid,
    drupal.content_type_product_video.field_facet_fitness_duration_value,
    drupal.content_type_product_video.field_facet_yoga_duration_value,
    drupal.content_type_product_video.field_facet_interview_collection_value,
    drupal.content_type_product_video.field_facet_interview_topic_value
   FROM drupal.content_type_product_video
  WHERE ((((drupal.content_type_product_video.field_facet_fitness_duration_value IS NOT NULL) 
     OR (drupal.content_type_product_video.field_facet_yoga_duration_value IS NOT NULL)) 
     OR (drupal.content_type_product_video.field_facet_interview_collection_value IS NOT NULL)) 
     OR (drupal.content_type_product_video.field_facet_interview_topic_value IS NOT NULL));