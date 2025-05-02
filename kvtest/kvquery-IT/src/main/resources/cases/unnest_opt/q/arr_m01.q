#
# any-min < eq-max : cannot push both, the eq pred is preferred to push
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.number
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      600 <any $t.address.phones[].areacode and $phone.areacode <= 620
