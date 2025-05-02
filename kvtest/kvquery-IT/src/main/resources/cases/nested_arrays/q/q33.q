select id
from foo f
where exists f.info.maps.key1.array[][][15 < $element and $element < 20]
