compiled-query-plan

{
"query file" : "in_expr/q/pq04.q",
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
      "target table" : "foo",
      "row variable" : "$$f",
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
          "field name" : "foo2",
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
            "value" : 6
          },
          {
            "iterator kind" : "CONST",
            "value" : 3.8
          }
        ],
        [
          {
            "iterator kind" : "CONST",
            "value" : 4
          },
          {
            "iterator kind" : "CONST",
            "value" : 3.5
          }
        ],
        [
          {
            "iterator kind" : "CONST",
            "value" : 4
          },
          {
            "iterator kind" : "CONST",
            "value" : 3.6
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