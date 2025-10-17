compiled-query-plan
{
"query file" : "joins/q/lina17.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0, 1 ],
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "SINGLE_PARTITION",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "A.B",
        "row variable" : "$$b",
        "index used" : "b_idx_b1",
        "covering index" : true,
        "index row variable" : "$$b_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "ancestor tables" : [
          { "table" : "A", "row variable" : "$$a", "covering primary index" : false }        ],
        "index filtering predicate" :
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b_idx"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 10
          }
        },
        "position in join" : 0
      },
      "FROM variables" : ["$$a", "$$b_idx"],
      "WHERE" : 
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "b1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "a1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        }
      },
      "SELECT expressions" : [
        {
          "field name" : "ida",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b_idx"
            }
          }
        },
        {
          "field name" : "idb",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#idb",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b_idx"
            }
          }
        }
      ]
    }
  }
}
}
