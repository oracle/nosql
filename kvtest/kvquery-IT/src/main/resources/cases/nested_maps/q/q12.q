select id
from foo f
where exists f.info.addresses.phones[$element.values().values().areacode = 650]
