compiled-query-plan

{
"query file" : "in_expr/q/oq03.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "order by fields at positions" : [ 1, 2 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_bar1234",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":3.5},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":3,"info.bar2":3.5},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.5},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.5},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":3.6},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":null},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":"EMPTY"},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":3.4},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":3.0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":3,"info.bar2":3.6},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":3,"info.bar2":null},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":3,"info.bar2":"EMPTY"},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":3,"info.bar2":3.4},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":3,"info.bar2":3.0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.6},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":null},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":"EMPTY"},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.4},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.6},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":null},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":"EMPTY"},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.4},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.0},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      },
      {
        "field name" : "bar1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.bar1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      },
      {
        "field name" : "bar2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.bar2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      }
    ]
  }
}
}