select id
from foo f
where exists f.info.maps[$element.values().foo =any "hhh"]
