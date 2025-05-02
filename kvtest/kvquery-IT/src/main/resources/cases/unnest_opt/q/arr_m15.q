#
# any-eq > value-min, any pred pushed, value pred applied 
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $areacode
from foo as $t, $t.address.phones[].areacode as $areacode
where $t.address.state = "CA" and
      $areacode > 500 and
      $t.address.phones[].areacode =any 650
