select id, $phone.number
from User as $u, unnest($u.addresses[] as $address, $address.phones[][] as $phone)
where $address.state =any 'CA' and $phone.areacode = 104
