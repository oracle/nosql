compiled-query-plan

{
"query file" : "in_expr/q/q16.q",
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
      "index scans" : [
        {
          "equality conditions" : {"info.bar1":7,"info.bar2":3.5},
          "range conditions" : { "info.bar3" : { "start value" : "a", "start inclusive" : true, "end value" : "p", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":2,"info.bar2":3.5},
          "range conditions" : { "info.bar3" : { "start value" : "a", "start inclusive" : true, "end value" : "p", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.5},
          "range conditions" : { "info.bar3" : { "start value" : "a", "start inclusive" : true, "end value" : "p", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.5},
          "range conditions" : { "info.bar3" : { "start value" : "a", "start inclusive" : true, "end value" : "p", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":7,"info.bar2":3.9},
          "range conditions" : { "info.bar3" : { "start value" : "a", "start inclusive" : true, "end value" : "p", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":2,"info.bar2":3.9},
          "range conditions" : { "info.bar3" : { "start value" : "a", "start inclusive" : true, "end value" : "p", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.9},
          "range conditions" : { "info.bar3" : { "start value" : "a", "start inclusive" : true, "end value" : "p", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.9},
          "range conditions" : { "info.bar3" : { "start value" : "a", "start inclusive" : true, "end value" : "p", "end inclusive" : false } }
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
        }
      ],
      "right-hand-side expressions" : [
        [
          {
            "iterator kind" : "CONST",
            "value" : 7
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
            "value" : 3.9
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