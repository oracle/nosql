compiled-query-plan

{
"query file" : "nested_arrays/q/unnest02.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 1, 0 ],
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
        "row variable" : "$t",
        "index used" : "idx_areacode_number_long",
        "covering index" : true,
        "index row variable" : "$t_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$t_idx",
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
              "variable" : "$t_idx"
            }
          }
        },
        {
          "field name" : "areacode",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info.addresses[].phones[][][].areacode",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$t_idx"
            }
          }
        }
      ]
    }
  }
}
}