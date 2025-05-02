select /* FORCE_INDEX(Foo idx_areacode_kind) */ id
from foo f
where exists f.info.address.phones[$element.areacode < 408] and
      exists f.info.address.phones[$element.areacode >= 650 ]
