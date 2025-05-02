#
#               A
#              /|\
#             / | \
#            B  F  G
#           / \     \ 
#          C   D     H
#         /                     
#        E                      
#

# define string as PK
create table A (
    ida1 string, a2 integer, a3 string,
    primary key(ida1))

# define integer as PK
create table A.B (
    idb1 integer, idb2 integer, b3 string, b4 json,
    primary key(idb1, idb2)) 
    using ttl 8 hours

# define long as PK
create table A.B.C (
    idc1 long, idc2 long, c3 map(integer),
    primary key(idc1, idc2)) 
    using ttl 12 hours

# define double as PK
create table A.B.D (
    idd1 double, d2 double, d3 integer, d4 array(string),
    primary key(idd1))

# define enum as PK
create table A.B.C.E (
    ide1 enum(tok1, tok2, tok3, tok4), ide2 integer, e3 record(r1 integer, r2 string default 'Enum PK Test', r3 array(integer)),
    primary key(ide1, ide2))

# define float as PK
create table A.F (
    idf1 float, idf2 float, idf3 integer, f4 string, f5 map(json),
    primary key(idf1, idf2, idf3))

# define timestamp as PK
create table A.G (
    idg1 timestamp(9), g2 integer, g3 array(map(integer)), g4 timestamp(0) default '2018-02-01T10:45:00',
    primary key(idg1))

# define number as PK
create table A.G.H (
    idh1 number, idh2 number, h3 long, h4 map(array(integer)),
    primary key(idh1, idh2))

create index idx_a_a3 on A(a3)

create index idx_b_b4 on A.B(b4.comment as string)

create index idx_c_c3 on A.B.C(c3.keys(), c3.values())

create index idx_d_d23 on A.B.D(d2, d3)

create index idx_f_f4 on A.F(f4)

