declare $p3 long; // 10
select id
from foo $f
where $p3 <= partition($f)
