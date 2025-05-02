#Expression is map filter step with .values() and and $value implicit variable with is null in projection
select id,s.children.values($value > 10)  is null from sn s