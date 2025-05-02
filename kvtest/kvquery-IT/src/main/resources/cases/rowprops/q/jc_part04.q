select id
from boo $f
where partition($f) = 2 and id = 3
