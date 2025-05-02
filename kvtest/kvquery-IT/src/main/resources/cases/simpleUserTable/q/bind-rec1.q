declare $vrec1 RECORD( city STRING,
                       state STRING,
                       phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
                       ptr STRING);

select * from Users where Users.address.city = $vrec1.city AND
                          Users.address.state = $vrec1.state