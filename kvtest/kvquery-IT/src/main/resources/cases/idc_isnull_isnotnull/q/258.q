#Expression has variable reference with is not null in projection
declare $vrec1 RECORD( city STRING,
                       state STRING,
                       phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
                       ptr STRING);
select id,$vrec1.city is not null from sn s where s.address.city = $vrec1.city OR
                          s.address.state = $vrec1.state 
