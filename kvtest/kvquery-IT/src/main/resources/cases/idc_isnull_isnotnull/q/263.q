#Expression is map filter step with .values() and $ ,$values as implicit variable with is not null in projection
select id, p.address.keys($.city != "Boston" and $value = "city") is not null from sn p