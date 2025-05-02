declare $name1 string; // "name_9999999999999999abcdefghijklmnopqrstuvwxyz"

// add new row to make a row with large row_storage_size, different than the initial rows
insert into foo values (99,$name1,57,"2015-01-01T10:45:00.0102345")

select /*+ FORCE_INDEX(foo idx_row_size) */ id, name
from foo $f
where  row_storage_size($f) > 80
