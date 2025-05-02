select id
from foo $f
where partition($f) <= 10
