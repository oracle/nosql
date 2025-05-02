select id
from foo f
where exists f.info.address.phones[$element.areacode in (415, 650, 570)].kind
