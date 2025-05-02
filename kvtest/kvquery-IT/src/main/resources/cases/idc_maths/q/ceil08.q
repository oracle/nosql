#ceil of array types and array in json
select t.numArr, ceil(t.numArr) as ceilnumArr,
       t.num2DArr, ceil(t.num2DArr) as ceilnum2DArr,
       t.num3DArr, ceil(t.num3DArr) as ceilnum3DArr,
       t.douArr, ceil(t.numArr) as ceildouArr,
       t.dou2DArr, ceil(t.num2DArr) as ceildou2DArr,
       t.dou3DArr, ceil(t.num3DArr) as ceildou3DArr,
                     t.doc.numArr as docnumArr, ceil(t.doc.numArr) as ceildocnumArr,
                     t.doc.num2DArr as docnum2DArr, ceil(t.doc.num2DArr) as ceildocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, ceil(t.doc.num3DArr) as ceildocnum3DArr,
                     t.doc.douArr as docdouArr, ceil(t.doc.numArr) as ceildocdouArr,
                     t.doc.dou2DArr as docdou2DArr, ceil(t.doc.num2DArr) as ceildocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, ceil(t.doc.num3DArr) as ceildocdou3DArr
 from functional_test t where id=1