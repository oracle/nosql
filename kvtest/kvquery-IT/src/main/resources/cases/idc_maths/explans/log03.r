compiled-query-plan

{
"query file" : "idc_maths/q/log03.q",
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
      "target table" : "functional_test",
      "row variable" : "$$functional_test",
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
    "FROM variable" : "$$functional_test",
    "SELECT expressions" : [
      {
        "field name" : "log_24_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 24
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_64_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 64
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_0_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_0_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_0_neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_neg4_neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -4
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_neg100_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -100
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_4_neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 4
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      }
    ]
  }
}
}