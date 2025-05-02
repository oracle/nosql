select t.age, t.address.city, sum(t.age + t.lng) from ComplexType t group by t.age, t.address.city LIMIT 2 OFFSET 1
