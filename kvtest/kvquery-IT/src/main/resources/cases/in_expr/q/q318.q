declare $arr12 array(json); // [ (8, "u", 100), (4, "v", 109), (6, "", 103) ]
select id
from foo f
where (f.info.bar1, f.info.bar3, f.info.bar4) in $arr12[]
