select count(p.flt),sum(p.children.values().age) as childAge from ComplexType p where p.age>=0 group by count(p.flt) order by count(p.children.values().friends)
