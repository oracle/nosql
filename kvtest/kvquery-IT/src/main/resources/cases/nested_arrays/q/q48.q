select id, f.info.maps.key1.array[][][]
from foo f
where exists f.info[$element.maps.key1.array[][][] >any 6] and
      f.info.maps.key1.array[][][] <any 3
