#
# any-eq < val-max, any pred pushed, value pred applied 
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $areacode
from foo as $t, $t.address.phones[].areacode as $areacode
where $t.address.state = "CA" and
      $areacode < 650 and
      $t.address.phones[].areacode =any 408
