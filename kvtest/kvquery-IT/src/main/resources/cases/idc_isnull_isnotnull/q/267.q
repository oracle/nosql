#Expression is map filter step with .values() and $ ,$key as implicit variable with is null in projection
select id,s.children.values().values($.age=10 and $key="age") is null from sn s