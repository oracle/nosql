compiled-query-plan

{
"query file" : "sec_index/q/sort15.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0, 1 ],
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
        "target table" : "Bar",
        "row variable" : "$$Bar",
        "index used" : "idx_str",
        "covering index" : true,
        "index row variable" : "$$Bar_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$Bar_idx",
      "SELECT expressions" : [
        {
          "field name" : "fld_sid",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#fld_sid",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Bar_idx"
            }
          }
        },
        {
          "field name" : "fld_id",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#fld_id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Bar_idx"
            }
          }
        },
        {
          "field name" : "fld_str",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "fld_str",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$Bar_idx"
            }
          }
        }
      ]
    }
  }
}
}