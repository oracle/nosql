select f.xact.state,
       seq_sort(array_collect([seq_transform(f.xact.items[], $.qty * $.price)])[]) as amounts,
       count(*) as cnt
from bar f
group by f.xact.state
