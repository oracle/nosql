compiled-query-plan

{
"query file" : "prim_index/q/bind7.q",
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
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "id1" : { "start value" : 4, "start inclusive" : true } }
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "LESS_THAN",
        "left operand" :
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$ext7_1"
        },
        "right operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo"
          }
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$foo",
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo"
          }
        }
      }
    ]
  }
}
}