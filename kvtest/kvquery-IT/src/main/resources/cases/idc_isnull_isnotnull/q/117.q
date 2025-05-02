#Expression is map filter step with .values() and and $value implicit variable with is null in predicate
select id,s.children.values($value= "age") is null  from sn s