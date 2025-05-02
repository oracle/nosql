select id
from foo f
where f.info.map1.values($key = "key1").map2.values() =any 35
