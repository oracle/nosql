compiled-query-plan

{
"query file" : "maths/q/ln01.q",
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
      "target table" : "math_test",
      "row variable" : "$$math_test",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$math_test",
    "SELECT expressions" : [
      {
        "field name" : "ln0",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "ln1",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "ln2",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "ln5",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "ln10",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 10
            }
          ]
        }
      },
      {
        "field name" : "ln100",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 100
            }
          ]
        }
      },
      {
        "field name" : "ln10000",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 10000
            }
          ]
        }
      },
      {
        "field name" : "lne100",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "lne10",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "ln05",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.5
            }
          ]
        }
      },
      {
        "field name" : "ln025",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.25
            }
          ]
        }
      },
      {
        "field name" : "ln0125",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.125
            }
          ]
        }
      },
      {
        "field name" : "ln1230456",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.456
            }
          ]
        }
      },
      {
        "field name" : "ln100000000000",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 100000000000
            }
          ]
        }
      },
      {
        "field name" : "lnneg1",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "lnneg05",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -0.5
            }
          ]
        }
      },
      {
        "field name" : "lnneg1000",
        "field expression" : 
        {
          "iterator kind" : "LN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1000
            }
          ]
        }
      }
    ]
  }
}
}