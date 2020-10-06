# Introduction 
This GO module consists of 4 packages:
- extractDb; functions to get changed data from MSSQL database
- transform; transform from a source-specific struct (resulting from a query on the source database), using a map-configuration, to one of the fhirmodels
- fhirModels; target structs (that includes popular Dutch/Nictiz FHIR extensions)
- loadFhir; marshal and send fhirmodel-instances to a Fhir-API 

# Getting Started

Should be used with a code-repository that contains queries and mappings for a specific care system and/or care provider 

# Build and Test
TODO (use Go test package https://golang.org/pkg/testing/ )