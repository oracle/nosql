select id, $phone.areacode
from User_json as $u, unnest($u.info.addresses.phones[][] as $phone)
order by id
