select id
from foo f
where exists f.info[exists $element.addresses[$element.state = "CA"] and
                    exists $element.addresses.phones[][][$element.areacode = 408 and
                                                         $element.kind = "work"]]
