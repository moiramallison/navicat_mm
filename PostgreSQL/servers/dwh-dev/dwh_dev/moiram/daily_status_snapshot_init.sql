drop table if exists moiram.daily_status_snapshot;


CREATE TABLE moiram.daily_status_snapshot (
day_timestamp timestamp(6),
gcsi_user_id int8,
subscription_id int8,
paid_through_date timestamp(6),
status text 
);

drop table if exists moiram.daily_status_y2015q4;

create table moiram.daily_status_y2015q4(
check ( day_timestamp >=  '20151001'::date and day_timestamp <  '20160101'::date)
) inherits (moiram.daily_status_snapshot);

create index idx_ds_y2015q4_ts on moiram.daily_status_y2015q4 (day_timestamp);
create index idx_ds_y2015q4_sid on moiram.daily_status_y2015q4 (subscription_id);
create index idx_ds_y2015q4_guid on moiram.daily_status_y2015q4 (gcsi_user_id);
create index idx_ds_y2015q4_pdate on moiram.daily_status_y2015q4 (paid_through_date);
create index idx_ds_y2015q4_status on moiram.daily_status_y2015q4 (status);
create index idx_ds_y2015q4_pdate_ts on moiram.daily_status_y2015q4 (day_timestamp, paid_through_date);


drop table if exists moiram.daily_status_y2016q1;

create table moiram.daily_status_y2016q1 (
check ( day_timestamp >=  '20160101'::date)
) inherits (moiram.daily_status_snapshot);

create index idx_ds_y2016q1_ts on moiram.daily_status_y2016q1 (day_timestamp);
create index idx_ds_y2016q1_sid on moiram.daily_status_y2016q1 (subscription_id);
create index idx_ds_y2016q1_guid on moiram.daily_status_y2016q1 (gcsi_user_id);
create index idx_ds_y2016q1_pdate on moiram.daily_status_y2016q1 (paid_through_date);
create index idx_ds_y2016q1_status on moiram.daily_status_y2016q1 (status);
create index idx_ds_y2016q1_pdate_ts on moiram.daily_status_y2016q1 (day_timestamp, paid_through_date);

create or replace function daily_status_insert_trigger()
returns trigger as $$
begin 
   if  (NEW.day_timestamp >=  '20160101'::date) then
      insert into moiram.daily_status_y2016q1 values (NEW.*);
   elsif (NEW.day_timestamp >=  '20151001'::date and 
          NEW.day_timestamp <  '2016-01-01'::date) then
      insert into moiram.daily_status_y2015q4 values (NEW.*);
   end if;
   return null;
end;
$$
language plpgsql;

create trigger insert_dss_trigger
    before insert on moiram.daily_status_snapshot
    for each row execute procedure daily_status_insert_trigger();
    
