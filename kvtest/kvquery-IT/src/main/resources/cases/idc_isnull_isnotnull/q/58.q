#Expression returns json int with is not null in predicate
select id from sn s where s.children.Lisa.age is not null