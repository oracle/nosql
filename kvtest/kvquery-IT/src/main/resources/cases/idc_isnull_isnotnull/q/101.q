#Expression is map filter step with .keys() and $key implicit variable with is null in predicate
select id,s.children.keys() from sn s where s.children.keys($key = "Fred") is null
