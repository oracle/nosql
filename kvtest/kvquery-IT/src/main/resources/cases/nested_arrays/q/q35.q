select id
from foo f
where exists f.info.maps.values().array[]
