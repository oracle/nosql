select id
from foo $f
where 10 < partition($f)
