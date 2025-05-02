select id, f.info.addresses.state
from foo f
where exists f.info[exists $element.addresses[$element.state = "OR"]] and
      exists f.info.addresses.phones[][][$element.areacode = 118 and
                                         $element.kind = "work"]
