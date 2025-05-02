declare $arr5 array(string); // [ "1abcdefghijk2lmnopqrstuvwxyz3", "NAME_12345", "name_123", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "AAABCDEFGHIJKLMNOPQRSTUVWXYZzz", "AAname_123z" ]

select /*+ FORCE_INDEX(foo idx_trim_name) */ id, name, trim(name) as trim_name
from foo f
where trim(f.name) in $arr5[]
