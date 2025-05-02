###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###
CREATE TABLE Foo(
   id INTEGER,
   g  LONG,
   rec RECORD(a INTEGER,
              b ARRAY(INTEGER),
              c MAP(RECORD(ca INTEGER, cb INTEGER, cc INTEGER, cd INTEGER)),
              d ARRAY(MAP(INTEGER)),
              f FLOAT),
primary key(id))

CREATE INDEX idx1_a_c_c_f ON Foo (rec.a, KeYoF(rec.c), rec.c.vAlues().ca, rec.f)

CREATE INDEX idx2_ca_f_cb_cc_cd ON Foo (rec.c.Keys(),
                                        ElemEntOF(rec.c).ca,
                                        rec.f,
                                        rec.c.vAlues().cb,
                                        elementof(rec.c).cc,
                                        rec.c.VALUES().cd)

CREATE INDEX idx3_ca_f_cb_cc_cd ON Foo (rec.c.Values().ca,
                                        rec.c.keYs(),
                                        rec.f,
                                        rec.c.valUEs().cb,
                                        ElemEntOF(rec.c).cc,
                                        rec.c.values().cd)

create index idx4_c1_keys_vals_c3 on Foo (rec.c.c1.ca,
                                          rec.c.Keys(),
                                          rec.c.vaLues().ca,
                                          rec.c.c3.cb)

CREATE INDEX idx5_g_c_f ON Foo (g, rec.c.values().ca, rec.f)

create index idx6_c1_c2_c3 on Foo (rec.c.c1.ca, rec.c.c2.ca, rec.c.c3.cb)

create index idx7_c1_c3 on Foo (rec.c.c1.ca, rec.c.c3.cd)


CREATE TABLE Boo(
   id INTEGER,
   expenses MAP(INTEGER),
primary key(id))

create index idx on boo (expenses.food)

create index idx2 on boo (expenses."%%fo_od")

create index idx3 on boo (expenses."")

create index idx4 on boo (expenses."\"")

create index idx6 on boo (expenses.".foo", expenses."foo[")

create index idx7 on boo (expenses."[]")

create index idx8 on boo(expenses.keys())
