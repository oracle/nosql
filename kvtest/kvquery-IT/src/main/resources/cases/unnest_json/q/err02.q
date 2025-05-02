select /* FORCE_PRIMARY_INDEX(Foo) */id, $areacode
from foo as $t, unnest($t.info.address $address,
                       $address.phones[].areacode as $areacode)
where $address.state = "MA" and
      $areacode < 800 and
      $areacode = 500
