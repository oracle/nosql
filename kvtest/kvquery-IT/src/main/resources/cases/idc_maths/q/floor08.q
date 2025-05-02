#floor of array types and array in json
select t.numArr, floor(t.numArr) as floornumArr,
       t.num2DArr, floor(t.num2DArr) as floornum2DArr,
       t.num3DArr, floor(t.num3DArr) as floornum3DArr,
       t.douArr, floor(t.numArr) as floordouArr,
       t.dou2DArr, floor(t.num2DArr) as floordou2DArr,
       t.dou3DArr, floor(t.num3DArr) as floordou3DArr,
                     t.doc.numArr as docnumArr, floor(t.doc.numArr) as floordocnumArr,
                     t.doc.num2DArr as docnum2DArr, floor(t.doc.num2DArr) as floordocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, floor(t.doc.num3DArr) as floordocnum3DArr,
                     t.doc.douArr as docdouArr, floor(t.doc.numArr) as floordocdouArr,
                     t.doc.dou2DArr as docdou2DArr, floor(t.doc.num2DArr) as floordocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, floor(t.doc.num3DArr) as floordocdou3DArr
 from functional_test t where id=1