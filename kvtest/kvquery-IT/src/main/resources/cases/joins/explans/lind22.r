compiled-query-plan

{
"query file" : "joins/q/lind22.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 4, 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "A",
        "row variable" : "$$a",
        "index used" : "a_idx_c1",
        "covering index" : true,
        "index row variable" : "$$a_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "descendant tables" : [
          { "table" : "A.B", "row variable" : "$$b", "covering primary index" : false }
        ],
        "position in join" : 0
      },
      "FROM variables" : ["$$a_idx", "$$b"],
      "SELECT expressions" : [
        {
          "field name" : "a_ida",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a_idx"
            }
          }
        },
        {
          "field name" : "b_ida",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        },
        {
          "field name" : "b_idb",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idb",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        },
        {
          "field name" : "b_c1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        },
        {
          "field name" : "sort_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a_idx"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "a_ida",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "a_ida",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "b_ida",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "b_ida",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "b_idb",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "b_idb",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "b_c1",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "b_c1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}