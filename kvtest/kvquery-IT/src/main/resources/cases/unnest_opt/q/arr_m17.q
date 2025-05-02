#
# value-eq > value-min, value eq pred pushed, value min pred can be eliminated
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $areacode
from foo as $t, $t.address.phones[].areacode as $areacode
where $t.address.state = "CA" and
      $areacode = 650 and
      $areacode > 408 
