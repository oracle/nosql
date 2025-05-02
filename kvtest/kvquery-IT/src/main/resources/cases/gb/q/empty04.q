select sum(f.xact.item.discount) as sum,
       count(*) as cnt,
       avg(id3) as avg,
       array_collect(f.xact.item.discount) as collect
from Foo f
where id1 < 0

