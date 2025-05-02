#
# val-max < any-max, value pred is stricter, any pred can be eliminated
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.areacode
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      $phone.areacode <= 500 and
      $t.address.phones[].areacode <any 700
