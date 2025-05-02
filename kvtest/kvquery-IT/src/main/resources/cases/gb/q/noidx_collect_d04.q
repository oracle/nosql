select
  f.xact.year,
  array_collect(distinct
                seq_transform(f.xact.items[], cast($.qty*$.price as long))) as amounts,
  count(*) as cnt
from bar f
group by f.xact.year
