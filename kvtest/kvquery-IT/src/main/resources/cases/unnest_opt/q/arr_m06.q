#
# val-min > any-min, value pred is stricter, any pred can be eliminated
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.areacode
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      600 < $phone.areacode and
      408 <any $t.address.phones[].areacode
