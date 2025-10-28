select id
from bar $f
where row_metadata($f).address.state = "CA" and
      row_metadata($f).address.phones.areacode =any 650
