compiled-query-plan

{
"query file" : "map_index/q/in09.q",
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
      "target table" : "Foo",
      "row variable" : "$$t",
      "index used" : "idx2_ca_f_cb_cc_cd",
      "covering index" : false,
      "index row variable" : "$$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"rec.c.Keys()":"c1","rec.c.values().ca":1,"rec.f":4.5},
          "range conditions" : { "rec.c.vAlues().cb" : { "end value" : 33, "end inclusive" : true } }
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "GREATER_OR_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec.c.values().cc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 101
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "IN",
          "left-hand-side expressions" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ca",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "c2",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "c",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "rec",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$t"
                    }
                  }
                }
              }
            }
          ],
          "right-hand-side expressions" : [
            {
              "iterator kind" : "CONST",
              "value" : 10
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        },
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "cb",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "c2",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "c",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "rec",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 20
          }
        },
        {
          "iterator kind" : "LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "cb",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "c2",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "c",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "rec",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 42
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
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "c1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "c1",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "rec",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          }
        }
      }
    ]
  }
}
}