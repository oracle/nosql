#
# TODO ???? The [$element.kind = "home"] pred could be pushed to the index
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.number, $phone.kind
from foo as $t, $t.address.phones[$element.kind = "home"] as $phone
where $t.address.state = "MA" and
      500 <= $phone.areacode and $phone.areacode < 600
