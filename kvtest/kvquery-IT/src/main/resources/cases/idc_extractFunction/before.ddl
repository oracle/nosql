###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE Extract(
        id INTEGER,
        ts0 TIMESTAMP(0),
		ts1 TIMESTAMP(1),
		ts2 TIMESTAMP(2),
		ts3 TIMESTAMP(3),
		ts4 TIMESTAMP(4),
		ts5 TIMESTAMP(5),
		ts6 TIMESTAMP(6),
		ts7 TIMESTAMP(7),
		ts8 TIMESTAMP(8),
		ts9 TIMESTAMP(9),
        rec RECORD(
                tsr0 TIMESTAMP(0),
                tsr1 TIMESTAMP(1),
	        tsr2 TIMESTAMP(2),
	        tsr3 TIMESTAMP(3),
		tsr4 TIMESTAMP(4),
	        tsr5 TIMESTAMP(5),
                tsr6 TIMESTAMP(6),
		tsr7 TIMESTAMP(7),
	        tsr8 TIMESTAMP(8),
	        tsr9 TIMESTAMP(9)
         ),                      
        mts9 MAP(TIMESTAMP(9)),
        ats9 ARRAY(TIMESTAMP(9)),
        s STRING,
	json JSON,
        PRIMARY KEY(id)
)
