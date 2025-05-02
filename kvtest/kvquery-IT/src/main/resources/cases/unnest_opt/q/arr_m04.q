#
# any-max < val-max, any pred is stricter, value pred CANNOT be eliminated
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.areacode
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      $t.address.phones[].areacode <any 500 and
      $phone.areacode <= 620
