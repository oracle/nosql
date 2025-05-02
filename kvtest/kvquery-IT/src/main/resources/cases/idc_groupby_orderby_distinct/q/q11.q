select count(p.flt),count(p.children.values().friends) as childAge from ComplexType p where p.age>=0 group by p.address.phones.work order by count(p.children.values().friends)
