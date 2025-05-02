select id, partition($f) as part
from foo $f
where partition($f) = 1
