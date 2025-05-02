select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as $t, unnest($t.info.address.phones[] as $phone)
where $t.info.address.state = "MA" and
      $phone.kind = "home" and
      $t.info.address.phones[].areacode =any 500

