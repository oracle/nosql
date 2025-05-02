select id, $phone.areacode
from User as $u, unnest($u.addresses.phones as $phone)
