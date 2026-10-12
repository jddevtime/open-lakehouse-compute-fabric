let
    Source = Sql.Database("srv", "erp"){[Schema="dbo",Item="T"]}[Data],
    Result = Table.RenameColumns(Source, {{"a","A"},{"b","B"}})
in
    Result
