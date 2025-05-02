#Expression is map filter step with .values() and $ ,$values as implicit variable with is null in projection
select id, p.address.values($.city != "Boston" and $value = "city") is null from sn p