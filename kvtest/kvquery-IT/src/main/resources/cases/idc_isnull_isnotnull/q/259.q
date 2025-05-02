#Expression is map filter step with .values() and $key,$values as implicit variable with is null in predicate
select id from sn p where  p.address.values($key != "city" and $value = "city") is null