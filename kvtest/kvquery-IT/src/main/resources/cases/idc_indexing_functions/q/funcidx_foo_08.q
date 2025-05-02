declare $arr4 array(string); // [ "  1abcdefghijk2lmnopqrstuvwxyz3 ", " name_12345  ", "  name_123 ", " abcdefghijklmnopqrstuvwxyz  ", "aaabcdefghijklmnopqrstuvwxyzzz", "aaname_123z" ]

select /*+ FORCE_INDEX(foo idx_lower_name) */ id, name, lower(name) as lower_case_name
from foo f
where lower(f.name) in $arr4[]
