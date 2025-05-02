#
# value-eq != any-eq, any eq pred pushed, value eq pred CANNOT be eliminated
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $areacode
from foo as $t, $t.address.phones[].areacode as $areacode
where $t.address.state = "CA" and
      $t.address.phones[].areacode =any 650 and
      $areacode = 408
