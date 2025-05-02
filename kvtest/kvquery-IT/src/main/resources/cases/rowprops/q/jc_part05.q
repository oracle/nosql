declare $ext2 integer; // 3
select id
from boo $f
where partition($f) = 2 and id = $ext2
