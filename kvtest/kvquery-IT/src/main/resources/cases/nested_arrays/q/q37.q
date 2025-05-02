select id
from foo f
where f.info.maps.values($key > "key7").array[][][$element < 7] =any 3
