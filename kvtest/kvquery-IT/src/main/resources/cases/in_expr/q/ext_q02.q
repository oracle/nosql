declare $k1 integer; // 6
        $k2 integer; // 3
        $k3 double;  // 3.4
select id
from foo f
where f.info.bar1 in ($k1, $k2, seq_concat(), $k1, seq_concat(), null) and
      f.info.bar2 in (3.5, 3.6, null, seq_concat(), $k3, 3.0, $k3, 3, null)
