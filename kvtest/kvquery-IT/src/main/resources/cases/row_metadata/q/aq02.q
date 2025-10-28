select id
from Foo $f
where row_metadata($f).address.state = "CA" and
      row_metadata($f).address.phones.areacode =any 650
