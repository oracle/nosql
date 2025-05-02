select f.xact.acctno, 
       f.xact.year, 
       avg(seq_transform (f.xact.items[] , $.price * $.qty)) as sales,
       count(seq_transform (f.xact.items[] , $.price * $.qty)) as cnt
from Bar f
group by f.xact.acctno, f.xact.year
order by avg(seq_transform(f.xact.items[] , $.price * $.qty)), f.xact.year
