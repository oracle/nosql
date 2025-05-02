#Expression is map filter step with .values() > operator and and $value implicit variable with is not null in projection
select id,s.children.values($value > 10)  is not null from sn s