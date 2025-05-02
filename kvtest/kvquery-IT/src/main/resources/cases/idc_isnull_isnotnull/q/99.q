#Expression is map filter step with .keys() with is null in projection
select id, s.children.George.age.keys() is null from sn s