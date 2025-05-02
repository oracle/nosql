#Expression is map filter step with .values() and $ ,$values as implicit variable with is null in predicate
select id from sn p where  p.address.values($.city != "Boston" and $value = "city") is null