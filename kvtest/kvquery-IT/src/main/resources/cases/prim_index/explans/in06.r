compiled-query-plan

{
"query file" : "prim_index/q/in06.q",
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
          "equality conditions" : {"id1":3},
          "range conditions" : { "id2" : { "start value" : 31.0, "start inclusive" : false, "end value" : 41.0, "end inclusive" : true } }
        },
        {
          "equality conditions" : {"id1":4},
          "range conditions" : { "id2" : { "start value" : 31.0, "start inclusive" : false, "end value" : 41.0, "end inclusive" : true } }
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