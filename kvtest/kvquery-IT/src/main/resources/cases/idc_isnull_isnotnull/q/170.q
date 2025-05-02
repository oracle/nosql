#Expression is array slice step with  H>array_size-1 in projection and is not null in projection
select id, s.address.phones[:14] is not null  from sn s