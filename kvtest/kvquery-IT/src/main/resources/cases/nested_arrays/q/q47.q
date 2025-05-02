select id
from foo f
where exists f.info[$element.maps.key1.array[][][] = 6]
