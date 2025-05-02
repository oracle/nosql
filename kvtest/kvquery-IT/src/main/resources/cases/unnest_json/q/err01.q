select /* FORCE_PRIMARY_INDEX(Foo) */id, $areacode
from foo as $t, unnest($t.info.address.phones[].areacode as $areacode)
where $t.info.address.state = "CA" and
      $areacode = 650 and
      $areacode < 408
