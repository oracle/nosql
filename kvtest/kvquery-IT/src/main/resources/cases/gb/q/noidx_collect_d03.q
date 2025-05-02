select f.xact.year,
       size(
           array_collect(distinct
                         seq_transform(f.xact.items[], cast($.qty * $.price as long)))
       ) as count_distinct,
       count(*) as cnt
from bar f
group by f.xact.year
