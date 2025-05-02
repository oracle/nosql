select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.number
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      600 < $phone.areacode and $phone.areacode <= 620
