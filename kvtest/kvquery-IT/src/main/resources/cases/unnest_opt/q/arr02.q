#
# Looks like covering index but is not
#
select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      $phone.areacode = 650
