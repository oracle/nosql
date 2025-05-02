declare $bool_true boolean;
select id
from bar b
where b.info.address.state in (3, "CA", $bool_true)
