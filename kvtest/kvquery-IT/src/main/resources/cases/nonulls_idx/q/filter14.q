select /* FORCE_INDEX(Foo idx_areacode_kind) */id
from foo f
where f.info.address[$element.phones.areacode =any 408 and
                     $element.phones.areacode =any 650 and
                     exists $element.phones[$element.areacode < 800]].phones.areacode >any 510
