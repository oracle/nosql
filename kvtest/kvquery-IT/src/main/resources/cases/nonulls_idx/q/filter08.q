select /* FORCE_INDEX(Foo idx_state_areacode_age) */id
from foo f
where f.info.address[$element.phones[$element.kind = "work"].areacode >any 408].state = "CA"
