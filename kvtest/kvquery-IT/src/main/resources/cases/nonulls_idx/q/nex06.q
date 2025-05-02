select id
from foo f
where exists f.info.address.phones[not exists $element.kind and
                                   $element.areacode = 415]
