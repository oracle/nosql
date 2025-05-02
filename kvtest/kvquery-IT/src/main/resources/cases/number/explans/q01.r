compiled-query-plan

{
"query file" : "number/q/q01.q",
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
      "target table" : "NumTable",
      "row variable" : "$$n",
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
    "FROM variable" : "$$n",
    "WHERE" : 
    {
      "iterator kind" : "GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "ARRAY_SLICE",
        "low bound" : 0,
        "high bound" : 0,
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "json",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$n"
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "num",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$n"
        }
      }
    },
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
            "variable" : "$$n"
          }
        }
      },
      {
        "field name" : "num",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "num",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$n"
          }
        }
      }
    ]
  }
}
}