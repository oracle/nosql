// add new row to make mod_time exp_times different than initial rows
insert into foo values (17,"name_17",79,"2015-01-01T10:45:00.0102345")
SET TTL 5 DAYS

select /*+ FORCE_INDEX(foo idx_exptime_ms) */ id, name, expiration_time_millis($f) > current_time_millis() as exp_time_millis_gt_cur_time
from foo $f
where expiration_time_millis($f) > current_time_millis() + 4 * 60 * 60 * 1000
