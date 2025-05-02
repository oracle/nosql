select $phone.areacode, avg(age) as age
from User as $u, unnest($u.addresses.phones[][] as $phone)
where 450 < $phone.areacode and $phone.areacode < 650
group by $phone.areacode
