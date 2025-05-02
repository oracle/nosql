#Expression has array constructor and is null in predicate
select id,age,s.address.phones from sn s where {"work" :[s.address.phones.keys($value>550)]} is null
