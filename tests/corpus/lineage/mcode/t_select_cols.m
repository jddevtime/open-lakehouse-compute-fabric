let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    Result = Table.SelectColumns(Source, {"a","b"})
in
    Result
