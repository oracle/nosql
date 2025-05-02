select id
from foo $f
where 0 <= partition($f) and partition($f) < 4
