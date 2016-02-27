/*  documenting prerequisites
\i facet groups.sql 
\i tag_groups.sql 
\i genre_groups.sql 
\i category groups_tmp.sql 
*/



drop table if exists common.media_tmp;

create table  common.media_tmp as
select cv.field_feature_nid_nid media_nid, 
       mn.vid media_vid,
       n.nid page_nid, 
       n.vid page_vid, 
       n.title,
       to_timestamp(n.created)::date created_date,
       va.admin_category,
       ac.site_segment,
       ac.reporting_segment    
from drupal.node n
join drupal.content_field_feature_nid cv
  ON n.vid = cv.vid
join drupal.node mn
  on cv.field_feature_nid_nid = mn.nid
left join common.v_admin_category va
    on n.vid = va.vid
left join common.admin_category_mapping ac
    on va.admin_category = ac.name
where n.type = 'product_video'
 and admin_category <> 'Radio';

--unstitched media
-- these can be associated with more than one guide.  I just get the first one
insert into common.media_tmp (media_nid, media_vid, page_nid, page_vid, title, site_segment, reporting_segment)
(select distinct on (media_nid)
       gd.media_nid, 
       gd.media_vid,
       gd.guide_day_nid,
       gd.guide_day_vid, 
       gd.guide_day_title,
       gd.site_segment,
       gd.site_segment
from common.guide_d gd
where guide_status = 1
  and media_vid not in 
    (select media_vid from common.media_tmp)
order by media_nid, guide_day_nid, guide_day_vid);

alter table common.media_tmp add feature varchar(10) default 'Feature';

insert into common.media_tmp
(select distinct on (field_preview_nid_nid)
       cv.field_preview_nid_nid, 
       mn.vid ,
       n.nid page_nid, 
       n.vid page_vid, 
       n.title,
       to_timestamp(n.created)::date created_date,
       va.admin_category,
       ac.site_segment,
       ac.reporting_segment,  
       'Preview' 
from drupal.node n
join drupal.content_field_preview_nid cv
  ON n.vid = cv.vid
join drupal.node mn
  on cv.field_preview_nid_nid = mn.nid
left join common.v_admin_category va
    on n.vid = va.vid
left join common.admin_category_mapping ac
    on va.admin_category = ac.name
where n.type = 'product_video'
 and admin_category <> 'Radio'
 and mn.vid not in 
    (select media_vid from common.media_tmp)
order by field_preview_nid_nid, mn.vid , n.nid, n.vid);

drop table if exists common.video_d cascade;

create table common.video_d as 
(select m.*,
        vco.content_origin,
        vn.duration,
        vs.series_title,
        vs.series_coverart_filepath,
        vf.season, 
        vf."cast",
        vf.copyright,
        vf.director,
        vf.studio,
        vf.long_description,
        tpm.field_media_maestro_title_code_value as mm_title_code,
        tpm.field_policy_id as media_policy_id,
        tpv.field_episode_value as episode,
        tpv.field_video_lang_value video_language,
        tpv.field_video_subtype_value as video_subtype,
        img1.filepath as coverart_filepath,
        pl.segment_id onboard_playlist_segment,
        -- gids are keys to bridge tables for multi_valued fields
        ng.gid as genre_gid,
        nt.gid as tag_gid,
        vc.gid as video_category_gid,
        nf.facet_focus   facet_focus_gid,
        nf.facet_style   facet_style_gid, 
        nf.facet_level   facet_level_gid,
        nf.facet_teacher facet_teacher_gid,  
        nf.facet_guest   facet_guest_gid, 
        nf.facet_host    facet_host_gid,  
        nf.facet_duration,  
        nf.facet_interview_collection,  
        nf.facet_interview_topic
from common.media_tmp m
left join common.v_content_origin vco
    on m.page_vid = vco.vid 
left join common.v_series vs
    on m.page_nid = vs.nid
left join common.v_node vn
    on m.media_nid = vn.nid
left join common.v_fields vf
    on m.page_nid = vf.nid
left join common.user_onboard_segments_playlist pl 
    on m.page_nid = pl.video_node
left join drupal.content_type_product_media tpm
    on m.media_vid = tpm.vid
left join drupal.content_type_product_video tpv
    on m.page_vid = tpv.vid
left join drupal.files img1 
    on tpv.field_coverart_image_fid = img1.fid
left join tmp.nid_genres ng
    on m.page_nid = ng.nid
left join tmp.nid_tags nt
    on m.page_nid = nt.nid
left join tmp.nid_categories vc
    on m.page_nid = vc.nid
left join tmp.nid_facets nf
    on m.page_nid = nf.nid
left join drupal.content_field_instructor cfi
    on m.page_nid  =cfi.nid

);


alter table common.video_d owner to dw_admin;

-- fix up duplicate preview rows
-- this gets me 95% of the way there.  The page data 
-- (nid, vid, title) still reflects the title of the first video 
-- when maybe it should be the series?


drop table if exists common.dupe_previews_tmp;

create table common.dupe_previews_tmp as 
(select t.*, 
        case when episode = first_episode then 1 else 0 end include
from
(select media_nid, 
        media_vid, 
        page_nid, 
        page_vid, 
        episode, 
        series_title,
        first_value(episode) over (partition by series_title order by episode) first_episode
 from common.video_d
where media_nid in 
   (select media_nid from common.video_d 
     group by media_nid
     having count(1) > 1))t);

delete from common.video_d
where page_nid in 
    (select page_nid from common.dupe_previews_tmp where include = 0);

select count(1), count(distinct media_nid) from common.video_d;

-- this  uses tables created in the pre-requisite files,
-- so this seems like a good place to put it.

drop table if exists common.solr_facets;

create table common.solr_facets as
select nid, string_agg(name, ',') facets from 
    (select nid, name from tmp.facets_tmp
      union 
      select nid, genres from tmp.genres_tmp
      union 
      select nid, tags from tmp.tags_tmp
      union 
      select nid, categories from tmp.categories_tmp) foo
group by nid;
   
alter table common.solr_facets owner to dw_admin;
