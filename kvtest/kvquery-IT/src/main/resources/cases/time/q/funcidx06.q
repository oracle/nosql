select /* FORCE_INDEX(foo idx_modtime) */ id1
from foo $f
where modification_time($f) < current_time()
