compiled-query-plan

{
"query file" : "time/q/funcidx11.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "bar2",
      "row variable" : "$$b",
      "index used" : "idx_year_month",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$b",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
        "input iterator" :
        {
          "iterator kind" : "PROMOTE",
          "target type" : "Any",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tmarr",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$b"
              }
            }
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 2021
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
            "variable" : "$$b"
          }
        }
      }
    ]
  }
}
}