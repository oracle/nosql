select id, $phone.areacode
from User as $u, unnest($u.addresses[] as $address, $address.phones[][] as $phone)
where $phone.kind = 'work'
