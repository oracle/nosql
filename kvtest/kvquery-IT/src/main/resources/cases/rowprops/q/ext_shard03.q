
declare $s1 integer; // = 1
select id,  partition($f) as part
from foo $f
where shard($f) = $s1
