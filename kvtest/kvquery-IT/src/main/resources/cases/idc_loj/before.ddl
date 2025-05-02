#
#               A
#              /|\
#             / | \
#            B  F  G
#           / \  \  \ 
#          C   D  Z   H
#         /           \           
#        E             I         
#			\
#			 J
#			  \
#			   K
#			    \
#			     L

# define string as PK
create table if not exists A (
    ida1 string, a2 integer, a3 string,
    primary key(ida1))

# define integer as PK
create table if not exists A.B (
    idb1 integer, idb2 integer, b3 string, b4 json,
    primary key(idb1, idb2)) 
    using ttl 8 hours

# define long as PK
create table if not exists A.B.C (
    idc1 long, idc2 long, c3 map(integer),
    primary key(idc1, idc2)) 
    using ttl 12 hours

# define integer as PK with complex data types in table
create table if not exists A.B.D (
    idd1 integer, d3 integer, d4 array(string),
    primary key(idd1))

# define enum as PK
create table if not exists A.B.C.E (
    ide1 enum(tok1, tok2, tok3, tok4), ide2 integer, e3 record(r1 integer, r2 string default 'Enum PK Test', r3 array(integer)),
    primary key(ide1, ide2))

# define float as PK
create table if not exists A.F (
    idf1 float, idf2 float, idf3 integer, f4 string, f5 map(json),
    primary key(idf1, idf2, idf3))

# just schema no data for this table
create table if not exists A.F.Z (
    idz1 integer, idz2 float, z1 string,
    primary key(idz1))

# define timestamp as PK
create table if not exists A.G (
    idg1 timestamp(9), g2 integer, g3 array(map(integer)), g4 timestamp(0) default '2018-02-01T10:45:00',
    primary key(idg1))

# define number as PK
create table if not exists A.G.H (
    idh1 number, idh2 number, h3 long, h4 map(array(integer)),
    primary key(idh1, idh2))

# define integer as PK
create table if not exists A.G.H.I (
    idi1 integer, i1 integer, i2 integer,
    primary key(idi1))

# can't define string as UUID as PK in cloud case because of key size constraint
create table if not exists A.G.H.I.J (
    idj1 string, j1 integer, j2 integer, j3 STRING as UUID,
    primary key(idj1))

# define integer as PK
create table if not exists A.G.H.I.J.K (
    idk1 integer, k1 integer, k2 integer,
    primary key(idk1))

#define integer as PK
create table if not exists A.G.H.I.J.K.L (
    idl1 integer, l1 integer, l2 integer,
    primary key(idl1))


