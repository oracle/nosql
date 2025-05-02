#
# value-eq < any-min, value pred pushed, any pred applied
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.areacode
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      $phone.areacode = 408 and
      $t.address.phones[].areacode >any 600
