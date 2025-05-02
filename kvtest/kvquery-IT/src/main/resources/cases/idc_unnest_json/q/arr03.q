select $phone.areacode, count(*) as age
from User_json as $u, unnest($u.info.addresses.phones[][] as $phone)
group by $phone.areacode
