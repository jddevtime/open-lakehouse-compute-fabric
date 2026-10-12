let
 S = AnalysisServices.Database("asazure://eastus.asazure.windows.net/srv", "model1", [Query="EVALUATE Sales"])
in
 S
