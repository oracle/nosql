#Expression is function call with size and is null in projection
select id,size(array) from sn s where size(map) is null or exists age order by id