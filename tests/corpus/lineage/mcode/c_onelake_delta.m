let
 S = DeltaLake.Table(AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/11111111-1111-4111-8111-111111111111/22222222-2222-4222-8222-222222222222/Tables/sales"))
in
 S
