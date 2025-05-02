select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as $t, unnest($t.info.address as $addr, $addr.phones[1:3] as $phone)
where $addr.state = "CA" and
      $phone.areacode = 650
