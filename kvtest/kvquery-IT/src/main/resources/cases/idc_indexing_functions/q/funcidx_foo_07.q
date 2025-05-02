declare $arr3 array(string); // [ "  1ABCDEFGHIJK2LMNOPQRSTUVWXYZ3 ", " NAME_12345  ",  "  NAME_123 ", " ABCDEFGHIJKLMNOPQRSTUVWXYZ  ", "AAABCDEFGHIJKLMNOPQRSTUVWXYZZZ", "AANAME_123Z"]

select /*+ FORCE_INDEX(foo idx_upper_name) */ id, name, upper(name) as upper_case_name
from foo f
where upper(f.name) in $arr3[]
