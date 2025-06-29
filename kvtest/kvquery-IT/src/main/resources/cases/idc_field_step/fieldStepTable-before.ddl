###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE Complex(
  id INTEGER,
  firstName STRING,
  lastName STRING,
  age INTEGER,
  ptr STRING,
  address RECORD( city STRING,
                  state STRING,
                  phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
                  ptr STRING,
         	  test STRING),
  children MAP( RECORD( age LONG, friends ARRAY(STRING) ) ),
  lng long , 
  flt float, 
  dbl double, 
  bool boolean,
  enm  ENUM(tok1, tok2, tok3, tok4),
  bin  BINARY,
  fbin  BINARY(10),
  primary key (id)
)
