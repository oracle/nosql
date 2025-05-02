compiled-query-plan

{
"query file" : "map_index/q/both17_cov.q",
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
      "covering index" : true,
      "index row variable" : "$$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"rec.c.Keys()":"c2"},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "EQUAL",
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
              "value" : 105
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "rec.c.VALUES().cd",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : -5
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "rec.c.vAlues().cb",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 11
            }
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$t_idx",
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
            "variable" : "$$t_idx"
          }
        }
      },
      {
        "field name" : "cc",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec.c.values().cc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        }
      },
      {
        "field name" : "cd",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec.c.VALUES().cd",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        }
      },
      {
        "field name" : "ca",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec.c.values().ca",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t_idx"
          }
        }
      }
    ]
  }
}
}