let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"a"}),
    Result = Table.AddIndexColumn(s, "idx", 1, 1)
in
    Result
