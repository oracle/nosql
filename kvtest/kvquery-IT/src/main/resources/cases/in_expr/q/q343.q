declare $arr13 array(json); // [(8, 100), (4, 109), (6, 103)]
select id
from foo f
where (f.info.bar1, f.info.bar4) in $arr13[] and
      f.info.bar1 in (4, 5)
