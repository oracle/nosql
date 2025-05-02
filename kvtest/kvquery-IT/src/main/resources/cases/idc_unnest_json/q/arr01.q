select id, $phone.number
from User_json as $u, unnest($u.info.addresses.phones[][] as $phone)
where $u.info.addresses.state =any "CA" and $phone.areacode = 480
