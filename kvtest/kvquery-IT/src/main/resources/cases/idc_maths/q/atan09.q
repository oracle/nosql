#atan of array types and array in json
select t.numArr, atan(t.numArr) as atannumArr,
       t.num2DArr, atan(t.num2DArr) as atannum2DArr,
       t.num3DArr, atan(t.num3DArr) as atannum3DArr,
       t.douArr, atan(t.numArr) as atandouArr,
       t.dou2DArr, atan(t.num2DArr) as atandou2DArr,
       t.dou3DArr, atan(t.num3DArr) as atandou3DArr,
                     t.doc.numArr as docnumArr, atan(t.doc.numArr) as atandocnumArr,
                     t.doc.num2DArr as docnum2DArr, atan(t.doc.num2DArr) as atandocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, atan(t.doc.num3DArr) as atandocnum3DArr,
                     t.doc.douArr as docdouArr, atan(t.doc.numArr) as atandocdouArr,
                     t.doc.dou2DArr as docdou2DArr, atan(t.doc.num2DArr) as atandocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, atan(t.doc.num3DArr) as atandocdou3DArr
 from functional_test t where id=1