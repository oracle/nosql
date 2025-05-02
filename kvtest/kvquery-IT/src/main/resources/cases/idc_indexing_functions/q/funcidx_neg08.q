select /*+ FORCE_INDEX(foo idx_trim_name) */ id
from foo f
where trim(name, "invalid_where_arg") = null
