CREATE TABLE IF NOT EXISTS USERS (
  id INTEGER,
  country STRING,
  firstName STRING,
  lastName STRING,
  otherNames ARRAY(RECORD(first STRING, last STRING)),
  age INTEGER,
  income LONG,
  lastLogin TIMESTAMP(4),
  address JSON,
  connections ARRAY(INTEGER),
  expenses MAP(NUMBER),
  type ENUM(free,paid),
  photo BINARY,
  thumbnail BINARY(3),
  rating DOUBLE,
  PRIMARY KEY(id)
)

CREATE INDEX IF NOT EXISTS countryIndex ON USERS (country)

CREATE INDEX IF NOT EXISTS stateIndex on USERS(address.state AS STRING)

CREATE TABLE IF NOT EXISTS emails (
    userId STRING,
    email STRING,
    userInfo JSON,
    PRIMARY KEY (userId)
)
                
                
CREATE TABLE IF NOT EXISTS emails.folder (
    folderID STRING,
    name STRING,
    PRIMARY KEY (name)
)
                                                            
CREATE TABLE IF NOT EXISTS emails.folder.message (
    messageId STRING,
    read BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (messageId)
)
