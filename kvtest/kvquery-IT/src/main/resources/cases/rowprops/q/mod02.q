select id, modification_time($f) > current_time()
from foo $f
