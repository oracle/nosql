compiled-query-plan

{
"query file" : "prim_index/q/ext_in01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$foo",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id1":0,"id2":30.0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"id1":4,"id2":0.0},
          "range conditions" : {}
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$x1"
        },
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$y1"
        }
      ],
      "map of key bind expressions" : [
        [ 0, -1 ],
        [ -1, 1 ]
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$foo",
    "SELECT expressions" : [
      {
        "field name" : "foo",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$foo"
        }
      }
    ]
  }
}
}