select /*+ FORCE_INDEX(foo idx_substring_name_pos_len) */ id
from foo f
where substring(f.name, -1) = null
