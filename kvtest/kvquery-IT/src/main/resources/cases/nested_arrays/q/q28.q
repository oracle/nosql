select /* FORCE_INDEX(Foo idx_areacode_kind) */id
from foo f
where f.info.addresses[exists $element.phones[$element.areacode =any 408]].phones.areacode >any 510
