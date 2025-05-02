select id1, year(modification_time($f)) > 2020
from foo $f
where modification_time($f) < current_time()
