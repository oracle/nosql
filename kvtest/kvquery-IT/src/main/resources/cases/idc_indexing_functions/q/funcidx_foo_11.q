declare $arr7 array(string); // [ "  1abcdefghijk2lmnopqrstuvwxyz3", " NAME_12345", "  name_123", " ABCDEFGHIJKLMNOPQRSTUVWXYZ", "AAABCDEFGHIJKLMNOPQRSTUVWXYZzz", "AAname_123z" ]

select /*+ FORCE_INDEX(foo idx_rtrim_name) */ id, name, rtrim(name) as rtrim_name
from foo f
where rtrim(f.name) in $arr7[]
