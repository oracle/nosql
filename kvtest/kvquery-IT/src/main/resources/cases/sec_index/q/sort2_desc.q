#
# range only
#
declare
$ext1 string; // "MA"

select id, age
from foo t
where $ext1 <= t.address.state
order by t.address.state desc, t.address.city desc, t.age desc
