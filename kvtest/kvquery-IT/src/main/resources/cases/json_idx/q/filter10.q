select /* FORCE_INDEX(Foo idx_areacode_kind) */id
from foo f
where f.info.address[exists $element.phones[$element.kind = "work"]].phones.areacode =any 408
