compiled-query-plan

{
"query file" : "idc_schemaless/q/q19.q",
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
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "VALUES",
              "predicate iterator" :
              {
                "iterator kind" : "AND",
                "input iterators" : [
                  {
                    "iterator kind" : "EQUAL",
                    "left operand" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$key"
                    },
                    "right operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : "popup"
                    }
                  },
                  {
                    "iterator kind" : "ANY_EQUAL",
                    "left operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "value",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "menuitem",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$value"
                        }
                      }
                    },
                    "right operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : "New"
                    }
                  }
                ]
              },
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "menu",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "menu",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$C"
                  }
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