select /*+ FORCE_INDEX(foo idx_substring_name_pos_len) */ id, substring(f.name, null)
from foo f
