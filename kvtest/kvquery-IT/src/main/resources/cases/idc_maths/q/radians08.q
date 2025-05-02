#radians of array types and array in json
select t.numArr, radians(t.numArr) as radiansnumArr,
       t.num2DArr, radians(t.num2DArr) as radiansnum2DArr,
       t.num3DArr, radians(t.num3DArr) as radiansnum3DArr,
       t.douArr, radians(t.numArr) as radiansdouArr,
       t.dou2DArr, radians(t.num2DArr) as radiansdou2DArr,
       t.dou3DArr, radians(t.num3DArr) as radiansdou3DArr,
                     t.doc.numArr as docnumArr, radians(t.doc.numArr) as radiansdocnumArr,
                     t.doc.num2DArr as docnum2DArr, radians(t.doc.num2DArr) as radiansdocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, radians(t.doc.num3DArr) as radiansdocnum3DArr,
                     t.doc.douArr as docdouArr, radians(t.doc.numArr) as radiansdocdouArr,
                     t.doc.dou2DArr as docdou2DArr, radians(t.doc.num2DArr) as radiansdocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, radians(t.doc.num3DArr) as radiansdocdou3DArr
 from functional_test t where id=1