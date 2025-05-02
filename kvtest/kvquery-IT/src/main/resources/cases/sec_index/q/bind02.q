declare $ext2 json; // 3
select id, age
from foo t
where $ext2 <= t.address.state
