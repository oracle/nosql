#Expression is array filter step with $pos implicit variable with is null in projection
select id,s.address.phones[$pos =0].work is null from sn s 