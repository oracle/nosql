compiled-query-plan

{
"query file" : "maths/q/idx_floor01.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "math_test",
        "row variable" : "$$t",
        "index used" : "idx_floor_dc",
        "covering index" : true,
        "index row variable" : "$$t_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "floor#dc" : { "start value" : 0.0, "start inclusive" : false } }
          }
        ],
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
          "field name" : "Column_2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "floor#dc",
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
}