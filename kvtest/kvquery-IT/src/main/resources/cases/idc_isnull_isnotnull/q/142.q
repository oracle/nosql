#Expression is array filter step with $pos implicit variable with is not null in projection
select id,s.address.phones[$pos >2] is not null from sn s