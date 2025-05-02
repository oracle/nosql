declare $ext1 string; // MA, ?-1 = 4, ?-2 = MA
select id, t.info.age, ? as qstn, $ext1
from foo t
where t.info.address.state = ?
