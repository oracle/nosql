select /*+ FORCE_INDEX(foo idx_trim_name) */ id
from foo f
where trim(name, null, null) = null
