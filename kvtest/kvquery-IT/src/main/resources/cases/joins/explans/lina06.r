compiled-query-plan

{
"query file" : "joins/q/lina06.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "SINGLE_PARTITION",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "A.B.C.D",
        "row variable" : "$$d",
        "index used" : "primary index",
        "covering index" : true,
        "index scans" : [
          {
            "equality conditions" : {"ida":40},
            "range conditions" : { "idb" : { "start value" : 0, "start inclusive" : false } }
          }
        ],
        "ancestor tables" : [
          { "table" : "A", "row variable" : "$$a", "covering primary index" : true },
          { "table" : "A.B", "row variable" : "$$b", "covering primary index" : true },
          { "table" : "A.B.C", "row variable" : "$$c", "covering primary index" : false }        ],
        "index filtering predicate" :
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idd",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$d"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 5
          }
        },
        "ON Predicate for table A.B.C" : 
        {
          "iterator kind" : "GREATER_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$c"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 15
          }
        },
        "position in join" : 0
      },
      "FROM variables" : ["$$a", "$$b", "$$c", "$$d"],
      "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "d_ida",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$d"
            }
          }
        },
        {
          "field name" : "d_idb",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idb",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$d"
            }
          }
        },
        {
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "d_ida",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "d_ida",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "d_idb",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "d_idb",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "cnt",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "cnt",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ]
}
}