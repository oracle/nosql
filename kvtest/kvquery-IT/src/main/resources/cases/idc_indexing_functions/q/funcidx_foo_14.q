declare $arr9 array(string); // [ "  1abcdefghijk2lmnopqrstuvwxyz3 ", " NAME_12345  ", "  name_123 ", " ABCDEFGHIJKLMNOPQRSTUVWXYZ  ", "BCDEFGHIJKLMNOPQRSTUVWXYZzz", "name_123z" ]

select /*+ FORCE_INDEX(foo idx_trim_name_trailing_z) */ id, name, trim(name, "trailing", "z") as trim_name_trailing_z
from foo f
where trim(f.name, "trailing", "z") in $arr9[]
