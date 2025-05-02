#degrees of array types and array in json
select t.numArr, degrees(t.numArr) as degreesnumArr,
       t.num2DArr, degrees(t.num2DArr) as degreesnum2DArr,
       t.num3DArr, degrees(t.num3DArr) as degreesnum3DArr,
       t.douArr, degrees(t.numArr) as degreesdouArr,
       t.dou2DArr, degrees(t.num2DArr) as degreesdou2DArr,
       t.dou3DArr, degrees(t.num3DArr) as degreesdou3DArr,
                     t.doc.numArr as docnumArr, degrees(t.doc.numArr) as degreesdocnumArr,
                     t.doc.num2DArr as docnum2DArr, degrees(t.doc.num2DArr) as degreesdocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, degrees(t.doc.num3DArr) as degreesdocnum3DArr,
                     t.doc.douArr as docdouArr, degrees(t.doc.numArr) as degreesdocdouArr,
                     t.doc.dou2DArr as docdou2DArr, degrees(t.doc.num2DArr) as degreesdocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, degrees(t.doc.num3DArr) as degreesdocdou3DArr
 from functional_test t where id=1