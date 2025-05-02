compiled-query-plan

{
"query file" : "rowprops/q/shard04.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "minimum partition id" : 2,
  "maximum partition id" : 10,
  "minimum shard id" : 1,
  "maximum shard id" : 1,
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$f",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$f",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "part",
        "field expression" : 
        {
          "iterator kind" : "FUNC_PARTITION",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      }
    ]
  }
}
}