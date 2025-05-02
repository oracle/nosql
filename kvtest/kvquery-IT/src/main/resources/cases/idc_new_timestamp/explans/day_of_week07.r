compiled-query-plan

{
"query file" : "idc_new_timestamp/q/day_of_week07.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "jsonCollection_test",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "l3",
        "field expression" : 
        {
          "iterator kind" : "CAST",
          "target type" : "Timestamp(3)",
          "quantifier" : "",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "l3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "l3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "s6",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "s6",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "s6",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      }
    ]
  }
}
}