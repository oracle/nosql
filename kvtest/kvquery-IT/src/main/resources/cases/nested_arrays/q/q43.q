select id
from foo f
where exists f.info.addresses[$element.state = "CA" and
                              exists $element.phones[][][$element.areacode = 408 and
                                                         $element.kind = "work"]]
