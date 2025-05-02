select b.xact.year, count(*)
from bar b
group by b.xact.year
order by avg(seq_transform(b.xact.items[$element.prod in ("milk", "cheese")],
                           $.qty * $.price))

