declare $name1 string; // "name_9999999999999999abcdefghijklmnopqrstuvwxyz"

insert into foo values (99,$name1,57,"2015-01-01T10:45:00.0102345")

select /*+ FORCE_INDEX(foo idx_length_name) */ id, name, length(f.name) as name_len
from foo f
where  length(f.name) > 30
