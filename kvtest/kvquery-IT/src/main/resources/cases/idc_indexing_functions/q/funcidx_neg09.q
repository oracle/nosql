select /*+ FORCE_INDEX(foo idx_trim_name) */ id
from foo f
where trim(f.name, -1) = null
