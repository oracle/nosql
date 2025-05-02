select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.number
from foo as $t, $t.address.phones[2:5] as $phone
where $t.address.state = "MA" and
      500 <= $phone.areacode and $phone.areacode < 600
