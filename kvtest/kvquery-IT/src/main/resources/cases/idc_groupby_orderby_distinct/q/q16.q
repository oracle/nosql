select p.flt from ComplexType p where p.age>=0 group by p.flt order by count(p.id)
