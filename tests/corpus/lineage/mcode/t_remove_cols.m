let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    Result = Table.RemoveColumns(Source, {"x"})
in
    Result
