select count(p.flt),sum(p.children.values().friends) as childAge from ComplexType p where p.age>=0 group by p.flt order by count(p.children.values().friends)
