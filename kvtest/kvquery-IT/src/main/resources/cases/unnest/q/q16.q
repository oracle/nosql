
select id, age
from foo as $t, $t.address.city as $city
where $city > "A" and $t.age > 10
