select id
from foo $f
where partition($f) = 2 and id = 3
