compiled-query-plan

{
"query file" : "idc_maths/q/jsonColl01.q",
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
        "field name" : "grid",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ABS",
              "input iterators" : [
                {
                  "iterator kind" : "PROMOTE",
                  "target type" : "Any",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "grid",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "address",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$t"
                      }
                    }
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sqrt1o001",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "SQRT",
              "input iterators" : [
                {
                  "iterator kind" : "PROMOTE",
                  "target type" : "Any",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "num2",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "numbers",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$t"
                      }
                    }
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      }
    ]
  }
}
}