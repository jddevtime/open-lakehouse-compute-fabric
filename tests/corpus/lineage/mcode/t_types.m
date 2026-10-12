let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    s = Table.SelectColumns(Source, {"a"}),
    Result = Table.TransformColumnTypes(s, {{"a", Int64.Type}})
in
    Result
