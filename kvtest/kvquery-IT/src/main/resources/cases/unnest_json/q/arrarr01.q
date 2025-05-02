select /* FORCE_INDEX(Foo idx_state_areacode_kind) */id
from foo as $t,
     unnest($t.info.address.phones[] as $phone),
     $t.info.address.phones[] as $phone2
where $t.info.address.state = "CA" and
      $phone.areacode = 650 and $phone2.kind = "home"
