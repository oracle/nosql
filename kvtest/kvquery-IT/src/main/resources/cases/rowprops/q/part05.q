declare $ext2 integer; // 3
select id
from foo $f
where partition($f) = 2 and id = $ext2
