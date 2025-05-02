select id
from foo f
where exists f.info.maps.values($key = "key7").array[][][$element < 7]
