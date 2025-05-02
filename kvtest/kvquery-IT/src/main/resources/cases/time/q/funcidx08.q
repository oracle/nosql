select id1, modification_time($f) < current_time() as mod_time
from foo $f
where modification_time($f) < current_time()
