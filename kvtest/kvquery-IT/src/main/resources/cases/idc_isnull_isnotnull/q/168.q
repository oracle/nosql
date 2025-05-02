#Expression is array slice step with L<0 only in projection and is null in projection
select id, s.address.phones[-4:] is null  from sn s