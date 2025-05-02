#
# value-eq > value-max
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $areacode
from foo as $t, $t.address $address, $address.phones[].areacode as $areacode
where $address.state = "CA" and
      $areacode < 800 and
      $areacode = 650
