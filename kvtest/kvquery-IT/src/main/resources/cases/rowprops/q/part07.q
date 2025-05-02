select id
from foo $f
where 2 <= partition($f) and partition($f) < 15
