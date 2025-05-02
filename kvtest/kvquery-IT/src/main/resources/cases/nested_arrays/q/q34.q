select id
from foo f
where exists f.info.maps.values().array[][][15 < $element and $element < 20]
