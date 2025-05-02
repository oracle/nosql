compiled-query-plan

{
"query file" : "idc_in_expr/q/q12.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "ComplexType",
      "row variable" : "$$f",
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
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "IN",
      "left-hand-side expressions" : [
        {
          "iterator kind" : "CAST",
          "target type" : "Long",
          "quantifier" : "",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        }
      ],
      "right-hand-side expressions" : [
        {
          "iterator kind" : "FN_SEQ_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VALUES",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "children",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            }
          }
        }
      ]
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
            "variable" : "$$f"
          }
        }
      },
      {
        "field name" : "sum",
        "field expression" : 
        {
          "iterator kind" : "FN_SEQ_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VALUES",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "children",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            }
          }
        }
      }
    ]
  }
}
}