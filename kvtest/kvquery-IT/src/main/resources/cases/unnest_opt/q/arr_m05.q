#
# val-min < any-min, any pred is stricter, value pred CANNOT be eliminated
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.areacode
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      408 < $phone.areacode and
      600 <any $t.address.phones[].areacode
