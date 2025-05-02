#Expression is map filter step with .keys() with is not null in predicate
select id, s.children.keys() from sn s where s.children.Anna.age.keys() is not null