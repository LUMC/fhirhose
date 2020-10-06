package fhirhose

import (
    "database/sql"
    "context"
    "log"
	"fmt"
)

// LsnMax fetch max Lsn in source database
func LsnMax(ctx context.Context, db  *sql.DB) (string){
var LsnMax sql.NullString
	err := db.QueryRowContext(ctx, "SELECT CONVERT(NVARCHAR(30),MAX([start_lsn]),1) AS LsnMax FROM cdc.lsn_time_mapping").Scan(&LsnMax)
	if err != nil {
		log.Fatal("Scan failed:", err.Error())
	}
	if LsnMax.Valid == false {
		LsnMax.String = "0x00000000000000000000"
	}
	return LsnMax.String
}

//Deduplicate receives multiple maps of ID's and returns a single map containing unique values
func Deduplicate(IDLists []map[int]string)(map[int]string){
	var counter int
	mTemp := make(map[string]int)

	for _, IDList := range IDLists {
		for _, ID := range IDList{
			_, ok := mTemp[ID]
			if ok==false {mTemp[ID] = counter}
			counter++
		}
	}

	m := make(map[int]string)
	counter = 1
	for ID := range mTemp {
		m[counter]=ID
		counter++
	}

	fmt.Println("aantal unieke CDC ID's: ", len(m))
	return m
}


//ClusterIDs serialises ID's for 'IN' clause in SQL-statement, E.g. 1234','2345','3456','5678
func ClusterIDs(uniqueIDList map[int]string, clusterSize int, IDDataTypeString bool)(map[int]string){
m := make(map[int]string)

listSize := len(uniqueIDList)
IDsINcount := 1
IDsINstring := ""
mLen := 1
var scheidingsTeken string
if IDDataTypeString {
	scheidingsTeken = "','"
} else {
	scheidingsTeken = ","
}
var resourceID string
for keyNr:=1 ; keyNr <= listSize ; keyNr++ {
	resourceID = uniqueIDList[keyNr]
		if IDsINcount < clusterSize && keyNr != listSize {
				IDsINstring += resourceID + scheidingsTeken //TODO: IDDataTypeString == false optie maken
			
			} else {
				IDsINstring += resourceID 
				m[mLen]=IDsINstring
				IDsINcount = 0
				IDsINstring = ""
				mLen++
				}
			IDsINcount++}
			return m
	}

// EventToResourceIDs load CDC rows and return a map 
func EventToResourceIDs(ctx context.Context, db  *sql.DB, fh  *sql.DB, sourceTable string, srcLsnMax string, srcQuery string) (map[int]string) { 
 	var counter int
 	m := make(map[int]string)
	
	// Run query and scan for result
	var fhLsnMax sql.NullString
	err := fh.QueryRowContext(ctx, "SELECT CONVERT(NVARCHAR(30),max(lsn),1) AS LsnMax FROM lsnMax WHERE sourceTable = @p1", sourceTable).Scan(&fhLsnMax)
	if err != nil {
		log.Fatal("Scan failed:", err.Error())
	}
	if fhLsnMax.Valid == false {
		fhLsnMax.String = "0x00000000000000000000"
	}
	lsnFilter := "AND (a.[__$start_lsn] <= " + srcLsnMax + ") AND (a.[__$start_lsn] > " + fhLsnMax.String + ")"

	rows, err := db.QueryContext(ctx, srcQuery + lsnFilter)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    for rows.Next() {
		var resourceID string
		var startLsn string
        if err := rows.Scan(&resourceID, &startLsn); err != nil {
            log.Fatal(err)
        }
		m[counter]= resourceID
		counter++
    }
    if err := rows.Err(); err != nil {
        log.Fatal(err)
	}

//hoog fhLsnMax op als het proces goed verlopen is
_, err = fh.ExecContext(ctx, "INSERT INTO lsnMax (sourceTable, lsn) VALUES (@p1, CONVERT(BINARY(10),@p2,1))", sourceTable, srcLsnMax)
if err != nil {
    log.Fatal(err)
}

	fmt.Println("aantal CDC ID's voor ", sourceTable,":", len(m))
	return m
}