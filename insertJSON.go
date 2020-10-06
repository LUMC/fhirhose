package fhirhose

import (
    "database/sql"
    "context"
    "log"
	"fmt"
)

//InsertJSONFiles inserts JSON files directly in the VONK database (should be send to FHIR-API via https)
func InsertJSONFiles(ctx context.Context, fh *sql.DB, data map[string][]byte){

    fmt.Println("aantal nieuwe JSON's om te inserten: ", len(data))

// Iterate numbers in the size batch we're looking for
chunkSize := 999
sqlStr := ""
sqlStrCount := 1

for ID, row := range data {
    sqlStr += "('" + ID + "','" + string(row) + "'),"
    if sqlStrCount == chunkSize {
        sqlStr = "INSERT INTO fhirJson (resourceId, JSON) VALUES " + sqlStr
        //trim the last ','
        sqlStr = sqlStr[0:len(sqlStr)-1]
    
        //insert all (chunksize) values at once
        _, err := fh.ExecContext(ctx, sqlStr)
        if err != nil {
        log.Fatal(err)
        }
        sqlStr = ""
        sqlStrCount = 1
    }
    sqlStrCount++
}
// Process last, potentially incomplete batch
if sqlStrCount > 0 {
    sqlStr = "INSERT INTO fhirJson (resourceId, JSON) VALUES " + sqlStr
    //trim the last ','
    sqlStr = sqlStr[0:len(sqlStr)-1]
    
    //insert all (chunksize) values at once
    _, err := fh.ExecContext(ctx, sqlStr)
    if err != nil {
    log.Fatal(err)
}
}

}