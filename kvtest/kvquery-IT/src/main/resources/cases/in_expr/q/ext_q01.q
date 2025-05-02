declare $k1 integer; // 6
        $k2 integer; // 3
select id
from foo f
where f.info.bar1 in ($k1, $k2, seq_concat(), $k2, null, $k1)
