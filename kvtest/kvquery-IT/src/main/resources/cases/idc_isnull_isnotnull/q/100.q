#Expression is map filter step with .keys() with is not null in projection
select id, s.children.George.age.keys() is not null from sn s