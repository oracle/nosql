select id, $phone.number
from User as $u, unnest($u.addresses[] as $address, $address.phones[][] as $phone)
where $address.state = 'CA' and
      450 < $phone.areacode and $phone.areacode < 650
