select id
from foo f
where exists f.info.address.phones[$element.areacode >= 415 and exists $element.kind]
order by id

