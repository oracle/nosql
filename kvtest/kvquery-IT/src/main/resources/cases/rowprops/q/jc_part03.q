select id, partition($f) as part
from boo $f
where partition($f) = 1
