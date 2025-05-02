#cos of array types and array in json
select t.numArr, cos(t.numArr) as cosnumArr,
       t.num2DArr, cos(t.num2DArr) as cosnum2DArr,
       t.num3DArr, cos(t.num3DArr) as cosnum3DArr,
       t.douArr, cos(t.numArr) as cosdouArr,
       t.dou2DArr, cos(t.num2DArr) as cosdou2DArr,
       t.dou3DArr, cos(t.num3DArr) as cosdou3DArr,
                     t.doc.numArr as docnumArr, cos(t.doc.numArr) as cosdocnumArr,
                     t.doc.num2DArr as docnum2DArr, cos(t.doc.num2DArr) as cosdocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, cos(t.doc.num3DArr) as cosdocnum3DArr,
                     t.doc.douArr as docdouArr, cos(t.doc.numArr) as cosdocdouArr,
                     t.doc.dou2DArr as docdou2DArr, cos(t.doc.num2DArr) as cosdocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, cos(t.doc.num3DArr) as cosdocdou3DArr
 from functional_test t where id=1