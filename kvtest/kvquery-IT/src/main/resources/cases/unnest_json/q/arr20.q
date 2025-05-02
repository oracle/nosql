select /* FORCE_INDEX(Foo idx_state_areacode_kind) */id
from foo as $t, unnest($t.info.address.phones[] as $phone)
where ($t.info.address.state,  $phone.areacode) in (("CA", 650), ("MA", 450))
