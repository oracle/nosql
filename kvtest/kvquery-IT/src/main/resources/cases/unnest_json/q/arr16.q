select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone1.areacode
from foo as $t,
            $t.info.address.phones[] as $phone1,
            $t.info.address.phones[] as $phone2
where $t.info.address.state = "CA" and
      $phone1.areacode = 650 and
      $phone2.kind = "home"
