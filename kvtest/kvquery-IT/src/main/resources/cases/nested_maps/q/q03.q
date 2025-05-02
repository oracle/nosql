select id
from foo f
where exists f.info.addresses[$element.state = "CA"].
             phones.values().values($value.areacode = 408 and $value.kind = "work")
