declare $arr6 array(string); // [ "1abcdefghijk2lmnopqrstuvwxyz3 ", "NAME_12345  ", "name_123 ", "ABCDEFGHIJKLMNOPQRSTUVWXYZ  ", "AAABCDEFGHIJKLMNOPQRSTUVWXYZzz", "AAname_123z" ]

select /*+ FORCE_INDEX(foo idx_ltrim_name) */ id, name, ltrim(name) as ltrim_name
from foo f
where ltrim(f.name) in $arr6[]
