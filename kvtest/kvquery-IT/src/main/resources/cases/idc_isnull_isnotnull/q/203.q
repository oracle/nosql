#Expression has logical and arithmetic operator with multiple is null, is not null in projection
select id,s.address.phones[].work >any 200 is not null or  s.address.phones[].work <any 500 is null and s.address.phones[].home!=any 50 from sn s
