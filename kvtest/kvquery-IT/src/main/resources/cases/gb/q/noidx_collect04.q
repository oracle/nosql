select f.xact.year,
       array_collect(seq_transform(f.xact.items[],
                                   cast($.qty * $.price as integer))) as amounts,
       count(*) as cnt
from bar f
group by f.xact.year
