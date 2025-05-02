
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
    ida integer, a1 integer, a2 integer, c1 integer,
    primary key(ida))


create table A.B (
    idb integer, b1 integer, b2 integer, c1 integer,
    primary key(idb))

create table A.B.C (
    idc integer, c1 integer, c2 integer, c3 integer,
    primary key(idc))

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
    primary key(idd))

create table A.G.J.K (
    idk integer, k1 integer, k2 integer,
    primary key(idk))


create index a_idx_a2 on A(a2)

create index a_idx_c1 on A(c1)

create index a_idx_c1_a2 on A(c1, a2)

create index a_idx_a1_a2_c1 on A(a1, a2, c1)

create index d_idx_d2 on A.B.C.D(d2)

create index d_idx_d2_idb_c3 on A.B.C.D(d2, idb, c3)


create table X (idx1 string, primary key(idx1))

create table X.Y (idy1 string, y2 string default 'default_value', primary key(idy1))


create table P (idp integer, a1 integer, arr array(integer), primary key(idp))

create table P.C (idc integer, a1 integer, arr array(integer), primary key(idc))
