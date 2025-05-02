select /*+ FORCE_INDEX(foo idx_state_areacode_kind) */id
from foo as $t, unnest($t.info.address.phones[] as $phone)
where $t.info.address.state = "WA" and
      exists $phone.areacode
