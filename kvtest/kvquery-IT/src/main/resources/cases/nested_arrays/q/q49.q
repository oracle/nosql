select id
from foo f
where f.info.maps[].values().foo =any "hhh"
