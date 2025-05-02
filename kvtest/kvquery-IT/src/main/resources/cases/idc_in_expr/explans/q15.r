compiled-query-plan

{
"query file" : "idc_in_expr/q/q15.q",
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
      "target table" : "ComplexType",
      "row variable" : "$$f",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"id":0},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
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
          }
        ],
        "right-hand-side expressions" : [
          {
            "iterator kind" : "CONST",
            "value" : 3.5
          },
          {
            "iterator kind" : "CONST",
            "value" : 10.5
          },
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
      "position in join" : 0
    },
    "FROM variable" : "$$f",
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