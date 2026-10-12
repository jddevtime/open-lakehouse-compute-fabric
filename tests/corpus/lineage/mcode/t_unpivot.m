let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"id","q1","q2"}),
    Result = Table.UnpivotOtherColumns(s, {"id"}, "quarter", "val")
in
    Result
