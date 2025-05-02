select id
from foo f
where exists f.info.address[not exists $element.phones[].kind and
                            $element.phones.areacode >=any 408]
