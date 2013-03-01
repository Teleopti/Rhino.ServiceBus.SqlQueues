cd Rhino.ServiceBus.SqlQueues

rmdir release /S /Q

mkdir release

.nuget\nuget.exe pack "Rhino.ServiceBus.SqlQueues.csproj" -Prop Configuration=Release -OutputDirectory release