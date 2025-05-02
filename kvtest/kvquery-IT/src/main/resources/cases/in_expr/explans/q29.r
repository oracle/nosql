compiled-query-plan

{
"query file" : "in_expr/q/q29.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_bar1234",
      "covering index" : false,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.bar1":7,"info.bar2":3.9},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":2,"info.bar2":3.9},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.9},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.9},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":7,"info.bar2":4.0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":2,"info.bar2":4.0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":4.0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":4.0},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "IN",
            "left-hand-side expressions" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info.bar2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              },
              {
                "iterator kind" : "FN_LTRIM",
                "input iterators" : [
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "info.bar3",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f_idx"
                    }
                  }
                ]
              }
            ],
            "right-hand-side expressions" : [
              [
                {
                  "iterator kind" : "CONST",
                  "value" : 3.9
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "d"
                }
              ],
              [
                {
                  "iterator kind" : "CONST",
                  "value" : 4.0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "g"
                }
              ]
            ]
          },
          {
            "iterator kind" : "LESS_OR_EQUAL",
            "left operand" :
            {
              "iterator kind" : "CONST",
              "value" : 100
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.bar4",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            }
          },
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.bar4",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 108
            }
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "IN",
      "left-hand-side expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "foo1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "bar2",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        },
        {
          "iterator kind" : "FN_LTRIM",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.bar3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            }
          ]
        }
      ],
      "right-hand-side expressions" : [
        [
          {
            "iterator kind" : "CONST",
            "value" : 4
          },
          {
            "iterator kind" : "CONST",
            "value" : 3.9
          },
          {
            "iterator kind" : "CONST",
            "value" : "d"
          }
        ],
        [
          {
            "iterator kind" : "CONST",
            "value" : 3
          },
          {
            "iterator kind" : "CONST",
            "value" : 4.0
          },
          {
            "iterator kind" : "CONST",
            "value" : "g"
          }
        ]
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
      }
    ]
  }
}
}