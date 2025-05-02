
#
#                   A
#                /  |  \
#               /   |   \
#             B     F     G
#           /  \         /  \   
#          C    E       H    J
#         /                   \
#        D                     K
#

create table A (
    ida integer, a1 integer, a2 integer, c1 integer, str string,
    primary key(ida)) using ttl 5 days


create table A.B (
    idb integer, b1 integer, b2 integer, c1 integer,
    primary key(idb)) using ttl 5 days

create table A.B.C (
    idc integer, c1 integer, c2 integer, c3 integer, str string,
    primary key(idc)) using ttl 5 days

create table A.F (
    idf integer, f1 integer, f2 integer, c2 integer,
    primary key(idf))

create table A.B.E (
    ide integer, e1 integer, c1 integer, c2 integer,
    primary key(ide))

create table A.G (
    idg integer, g1 integer, g2 integer, c1 integer,
    primary key(idg))

create table A.G.J (
    idj integer, j1 integer, c3 integer, c2 integer,
    primary key(idj))

create table A.G.H (
    idh integer, j1 integer, c3 integer, c2 integer,
    primary key(idh))

create table A.B.C.D (
    idd integer, d1 integer, d2 integer, c3 integer, str string, 
    primary key(idd)) using ttl 5 days

create table A.G.J.K (
    idk integer, k1 integer, k2 integer,
    primary key(idk))


create index a_idx_a2 on A(a2)

create index a_idx_c1_a2 on A(c1, a2)

create index d_idx_d2 on A.B.C.D(d2)

create index d_idx_d2_idb_c3 on A.B.C.D(d2, idb, c3)

create index d_idx_ida_c3 on A.B.C.D(ida, c3)


