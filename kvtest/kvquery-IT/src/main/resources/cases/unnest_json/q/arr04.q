select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as $t, unnest($t.info.address.phones[] as $phone)
where $t.info.address.state = "CA" and
      $phone.areacode in (650, 408)
