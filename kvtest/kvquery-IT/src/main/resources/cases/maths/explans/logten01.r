compiled-query-plan

{
"query file" : "maths/q/logten01.q",
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
        "field name" : "logten0",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "logten1",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "logten2",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "logten5",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "logten10",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 10
            }
          ]
        }
      },
      {
        "field name" : "logten100",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 100
            }
          ]
        }
      },
      {
        "field name" : "logten10000",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 10000
            }
          ]
        }
      },
      {
        "field name" : "logten1000000000000",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1000000000000
            }
          ]
        }
      },
      {
        "field name" : "logten01",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "MULTIPLY_DIVIDE",
              "operations and operands" : [
                {
                  "operation" : "*",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 1
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 10.0
                  }
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "logten001",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "MULTIPLY_DIVIDE",
              "operations and operands" : [
                {
                  "operation" : "*",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 1
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 100.0
                  }
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "logten0001",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "MULTIPLY_DIVIDE",
              "operations and operands" : [
                {
                  "operation" : "*",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 1
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 1000.0
                  }
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "logten00001",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.0E-5
            }
          ]
        }
      },
      {
        "field name" : "logten1230456",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.456
            }
          ]
        }
      },
      {
        "field name" : "logtenneg1",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "logtenneg05",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -0.5
            }
          ]
        }
      },
      {
        "field name" : "logtenneg1000",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1000
            }
          ]
        }
      },
      {
        "field name" : "logtenneg01",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -0.1
            }
          ]
        }
      },
      {
        "field name" : "logtenpow",
        "field expression" : 
        {
          "iterator kind" : "LOG10",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1000000
                }
              ]
            }
          ]
        }
      }
    ]
  }
}
}