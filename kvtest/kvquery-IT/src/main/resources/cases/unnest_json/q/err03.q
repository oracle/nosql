select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as $t, unnest($t.info.address.phones[1:3] as $phone)
where $t.info.address.state = "CA" and
      $phone.areacode = 650
