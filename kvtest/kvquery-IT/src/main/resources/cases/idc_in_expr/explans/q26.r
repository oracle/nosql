compiled-query-plan

{
"query file" : "idc_in_expr/q/q26.q",
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
      "target table" : "ComplexType",
      "row variable" : "$$f",
      "index used" : "idx_flt",
      "covering index" : false,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"flt":10.5},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"flt":3.4},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"flt":3.6},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "IN",
        "left-hand-side expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "flt",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        ],
        "right-hand-side expressions" : [
          [
            {
              "iterator kind" : "CONST",
              "value" : 0
            },
            {
              "iterator kind" : "CONST",
              "value" : 10.5
            }
          ],
          [
            {
              "iterator kind" : "CONST",
              "value" : 4
            },
            {
              "iterator kind" : "CONST",
              "value" : 3.4
            }
          ],
          [
            {
              "iterator kind" : "CONST",
              "value" : 3
            },
            {
              "iterator kind" : "CONST",
              "value" : 3.6
            }
          ]
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "state",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "address",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "CA"
          }
        },
        {
          "iterator kind" : "IN",
          "left-hand-side expressions" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "flt",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "firstName",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          ],
          "right-hand-side expressions" : [
            [
              {
                "iterator kind" : "CONST",
                "value" : 0
              },
              {
                "iterator kind" : "CONST",
                "value" : 10.5
              },
              {
                "iterator kind" : "CONST",
                "value" : "first0"
              }
            ],
            [
              {
                "iterator kind" : "CONST",
                "value" : 4
              },
              {
                "iterator kind" : "CONST",
                "value" : 3.4
              },
              {
                "iterator kind" : "CONST",
                "value" : "first2"
              }
            ],
            [
              {
                "iterator kind" : "CONST",
                "value" : 3
              },
              {
                "iterator kind" : "CONST",
                "value" : 3.6
              },
              {
                "iterator kind" : "CONST",
                "value" : "first3"
              }
            ]
          ]
        },
        {
          "iterator kind" : "LESS_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "CONST",
            "value" : 10
          },
          "right operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        },
        {
          "iterator kind" : "LESS_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "lng",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 0
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
      }
    ]
  }
}
}