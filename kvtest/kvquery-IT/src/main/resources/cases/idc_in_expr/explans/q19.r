compiled-query-plan

{
"query file" : "idc_in_expr/q/q19.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "order by fields at positions" : [ 1 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "ComplexType",
        "row variable" : "$$f",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"id":0},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"id":1},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"id":2},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"id":3},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"id":4},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"id":5},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f",
      "WHERE" : 
      {
        "iterator kind" : "IN",
        "left-hand-side expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "bin",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        ],
        "right-hand-side expressions" : [
          {
            "iterator kind" : "CONST",
            "value" : "dGVzdGJpbmFyeQ=="
          }
        ]
      },
      "SELECT expressions" : [
        {
          "field name" : "f",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        },
        {
          "field name" : "sort_gen",
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
      ],
      "LIMIT" :
      {
        "iterator kind" : "CONST",
        "value" : 0
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "f",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "f",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ],
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 0
  }
}
}