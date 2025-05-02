select id
from foo $f
where 5 <= partition($f) and partition($f) < 4
