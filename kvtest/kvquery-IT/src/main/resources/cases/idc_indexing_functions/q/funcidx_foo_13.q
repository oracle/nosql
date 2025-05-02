declare $arr8 array(string); // [ "  1abcdefghijk2lmnopqrstuvwxyz3 ", " NAME_12345  ", "  name_123 ", " ABCDEFGHIJKLMNOPQRSTUVWXYZ  ", "BCDEFGHIJKLMNOPQRSTUVWXYZzz", "name_123z" ]

select /*+ FORCE_INDEX(foo idx_trim_name_leading_A) */ id, name, trim(name, "leading", "A") as trim_name_leading_A
from foo f
where trim(f.name, "leading", "A") in $arr8[]
