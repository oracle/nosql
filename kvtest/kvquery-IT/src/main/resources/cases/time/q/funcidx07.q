select id1
from foo $f
where modification_time($f) > current_time()
