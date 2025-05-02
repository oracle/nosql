select $phone.areacode, avg($u.info.age) as avg_age
from User_json as $u, unnest($u.info.addresses.phones[][] as $phone)
group by $phone.areacode
