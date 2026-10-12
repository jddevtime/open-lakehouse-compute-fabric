let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"a","b","c"}),
    Result = Table.ReorderColumns(s, {"c","a","b"})
in
    Result
