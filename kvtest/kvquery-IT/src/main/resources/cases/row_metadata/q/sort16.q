select id
from foo $f
where exists row_metadata($f).address.phones[$element.areacode >= 415 and
                                             exists $element.kind]
order by id
