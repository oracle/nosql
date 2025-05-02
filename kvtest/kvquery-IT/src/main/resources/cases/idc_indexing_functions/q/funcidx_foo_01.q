// add new row to make mod_time exp_times different than initial rows
insert into foo values (17,"name_17",79,"2015-01-01T10:45:00.0102345")
SET TTL 5 DAYS

select /*+ FORCE_INDEX(foo idx_modtime) */ id, name, modification_time($f) < current_time() as mod_time_lt_cur_time
from foo $f
where modification_time($f) < current_time()
