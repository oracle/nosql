compiled-query-plan

{
"query file" : "prim_index/q/in07.q",
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
          "equality conditions" : {"id1":3,"id2":30.0},
          "range conditions" : { "id3" : { "start value" : "tok0", "start inclusive" : false } }
        },
        {
          "equality conditions" : {"id1":4,"id2":42.0},
          "range conditions" : { "id3" : { "start value" : "tok0", "start inclusive" : false } }
        },
        {
          "equality conditions" : {"id1":1,"id2":12.0},
          "range conditions" : { "id3" : { "start value" : "tok0", "start inclusive" : false } }
        }
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