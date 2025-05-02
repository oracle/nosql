select /* FORCE_INDEX(Foo idx_areacode_kind) */id
from foo f
where f.info.address[exists $element.phones[$element.areacode = 408]].phones.areacode >any 510
