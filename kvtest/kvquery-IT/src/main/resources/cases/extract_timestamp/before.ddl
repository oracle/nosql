CREATE TABLE Foo(
	id INTEGER, 
	ts0 TIMESTAMP(0), 
	rec RECORD(ts1 TIMESTAMP(1), 
                   ts3 STRING), 
	mts6 MAP(TIMESTAMP(6)), 
	ats9 ARRAY(STRING), 
	s STRING, 
	PRIMARY KEY(id)
)
