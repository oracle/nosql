declare $p integer; // 1
select id,  partition($f) as part
from foo $f
where partition($f) = $p
