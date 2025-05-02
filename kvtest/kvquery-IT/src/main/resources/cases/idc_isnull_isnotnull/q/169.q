#Expression is array slice step with neither L nor H in projection and is not null in projection
select id, s.address.phones[:] is not null  from sn s