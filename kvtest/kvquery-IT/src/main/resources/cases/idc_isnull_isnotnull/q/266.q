#Expression is map filter step with .keys() and $ ,$values as implicit variable with is not null in predicate
select id from sn p where  p.address.keys($.state = "MA" and $value = "state") is not null