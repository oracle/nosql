#Expression returns id with json int and is null in predicate
select id from sn s where s.children.Lisa.age is null