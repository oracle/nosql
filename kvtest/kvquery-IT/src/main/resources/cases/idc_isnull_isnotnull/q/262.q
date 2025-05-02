#Expression is map filter step with .values() and $key,$values as implicit variable with is null in projection
select id, p.address.keys($.city != "Boston" and $value = "city") is null from sn p