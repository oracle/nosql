###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###
CREATE TABLE Bar(   
  id INTEGER,
  record RECORD(long LONG, int INTEGER, string STRING, bool BOOLEAN, float FLOAT),
  info JSON,
  primary key (id)
)

create index idx_state_city_age on bar (
    info.address.state as anyAtomic,
    info.address.city as anyAtomic,
    info.age as integer)

CREATE TABLE employee(
  id INTEGER,
  info JSON,
  primary key (id)
)

create index idx_age on employee (
    info.age as anyAtomic)

CREATE TABLE employee.skill(
  child_id INTEGER,
  info JSON,
  primary key (child_id)
)

create index idx_skill on employee.skill (
    info.skill as anyAtomic)

CREATE NAMESPACE Ns001

CREATE TABLE Ns001:employee(
  id INTEGER,
  info JSON,
  primary key (id)
)

create index idx_age_ns on Ns001:employee (
    info.age as anyAtomic)
