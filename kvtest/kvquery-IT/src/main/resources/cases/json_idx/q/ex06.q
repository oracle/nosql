select /* FORCE_INDEX(Foo idx_areacode_kind) */ id
from foo f
where exists f.info.address.phones[$element.areacode = 415 and
                                   not exists $element.kind]
