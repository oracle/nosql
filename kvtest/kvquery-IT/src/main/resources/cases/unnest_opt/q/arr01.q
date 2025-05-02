select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.number
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      $phone.areacode = 650
