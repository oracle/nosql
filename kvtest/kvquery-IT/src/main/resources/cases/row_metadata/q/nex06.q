select id
from foo $f
where exists row_metadata($f).address.phones[not exists $element.kind and
                                             $element.areacode = 415]
