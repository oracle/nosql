#Expression is map filter step with .values() and and $value implicit variable with is not null in projection
select id,s.children.values($value= "age") is not null  from sn s