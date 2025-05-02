select id
from boo $f
where 2 <= partition($f) and partition($f) < 15
