compiled-query-plan

{
"query file" : "uuid/q/q14.q",
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
      "target table" : "foo",
      "row variable" : "$$foo",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$foo",
    "WHERE" : 
    {
      "iterator kind" : "GREATER_OR_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "uid3",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$foo"
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "38ac"
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "uid3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "uid3",
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