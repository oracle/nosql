compiled-query-plan

{
"query file" : "sec_index/q/q10.q",
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
      "target table" : "Boo",
      "row variable" : "$$Boo",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "lastName",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Boo"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "xxx"
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$Boo",
    "SELECT expressions" : [
      {
        "field name" : "Boo",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$Boo"
        }
      }
    ]
  }
}
}