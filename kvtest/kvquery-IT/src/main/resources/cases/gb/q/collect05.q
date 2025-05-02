select f.xact.state,
       array_collect([seq_transform(f.xact.items[], $.qty * $.price)]) as amounts,
       count(*) as cnt
from bar f
group by f.xact.state
