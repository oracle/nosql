compiled-query-plan

{
"query file" : "in_expr/q/oq04.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "order by fields at positions" : [ 1, 2, 3 ],
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
          "equality conditions" : {"info.bar1":8,"info.bar2":3.9,"info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":4,"info.bar2":3.9,"info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":3.9,"info.bar3":""},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.9,"info.bar3":"d"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.9,"info.bar3":null},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":8,"info.bar2":null,"info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":8,"info.bar2":"EMPTY","info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":8,"info.bar2":3.1,"info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":8,"info.bar2":3.2,"info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":4,"info.bar2":null,"info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":4,"info.bar2":"EMPTY","info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":4,"info.bar2":3.1,"info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":4,"info.bar2":3.2,"info.bar3":"a"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":null,"info.bar3":""},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":"EMPTY","info.bar3":""},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":3.1,"info.bar3":""},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":3.2,"info.bar3":""},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":null,"info.bar3":"d"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":"EMPTY","info.bar3":"d"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.1,"info.bar3":"d"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY","info.bar2":3.2,"info.bar3":"d"},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":null,"info.bar3":null},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":"EMPTY","info.bar3":null},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.1,"info.bar3":null},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":3.2,"info.bar3":null},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
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
      },
      {
        "field name" : "bar3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.bar3",
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