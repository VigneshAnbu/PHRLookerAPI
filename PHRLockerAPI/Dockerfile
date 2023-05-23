#Get the base Image(Full .Net Core SDK)
FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build-env
WORKDIR /app

# Copy csproj and restore
COPY *.csproj ./
run dotnet restore


#Copy Everything else and build
COPY . ./
RUN dotnet publish -c Release -o out


#Generate runtime Image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app
EXPOSE 80
COPY --from=build-env /app/out .
ENTRYPOINT ["dotnet", "PHRLockerAPI.dll"]