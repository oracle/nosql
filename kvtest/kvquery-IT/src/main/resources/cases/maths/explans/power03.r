compiled-query-plan

{
"query file" : "maths/q/power03.q",
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
      "target table" : "math_test",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":8},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"id":9},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "x",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "dc",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "doc",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "powerx",
        "field expression" : 
        {
          "iterator kind" : "POWER",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "dc",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      }
    ]
  }
}
}