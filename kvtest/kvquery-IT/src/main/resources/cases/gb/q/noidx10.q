select f.xact.prodcat,
       sum(seq_transform(f.xact.items[] , $.price * $.qty)) as sales
from Bar f
group by f.xact.prodcat
