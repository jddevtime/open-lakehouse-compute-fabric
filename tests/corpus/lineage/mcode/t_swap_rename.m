let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    Result = Table.RenameColumns(Source, {{"a","b"},{"b","a"}})
in
    Result
