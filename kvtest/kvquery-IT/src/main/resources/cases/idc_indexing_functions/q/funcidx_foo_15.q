declare $arr10 array(string); // [ "bcd", "E_1", "me_", "DEF", "CDE" ]

select /*+ FORCE_INDEX(foo idx_substring_name_pos_len) */ id, name, substring(name, 4, 3) as substring_name_4_3
from foo f
where substring(f.name, 4, 3) in $arr10[]
