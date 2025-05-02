#Expression is array filter step with $pos implicit variable with is null in projection
select id,s.address.phones[$pos >2] is null from sn s
