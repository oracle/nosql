compiled-query-plan

{
"query file" : "idc_schemaless/q/q18.q",
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
      "target table" : "jsoncol",
      "row variable" : "$$C",
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
    "FROM variable" : "$$C",
    "SELECT expressions" : [
      {
        "field name" : "majorKey1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "majorKey1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$C"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : false,
          "input iterators" : [
            {
              "iterator kind" : "ARRAY_FILTER",
              "predicate iterator" :
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "home",
                  "input iterator" :
                  {
                    "iterator kind" : "ARRAY_SLICE",
                    "low bound" : 1,
                    "high bound" : 1,
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$"
                    }
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 11
                }
              },
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "phones",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$C"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}