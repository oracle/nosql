select p.flt,count(p.id) from ComplexType p where p.id>=0 group by p.flt order by count(p.id)
