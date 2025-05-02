select min(f.xact.item.discount) as min, f.record.long
from Foo f
group by f.record.long
