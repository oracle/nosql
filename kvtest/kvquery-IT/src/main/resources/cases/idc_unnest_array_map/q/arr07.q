select id, $phone.areacode
from User as $u, unnest($u.addresses.phones[][] as $phone)
where $u.addresses.state =any 'CA' and $phone.areacode <= 550
