package extractdb

import (
    "database/sql"
    "context"
    "log"
    "time"
	"fmt"
	"os"
    "github.com/joho/godotenv"
)

func getEnvVars() {
	err := godotenv.Load("../../databaseConnections.env")
	if err != nil {
		log.Fatal("Error loading .env file. Please have a look at the instructions in the /internal/config/databaseConnections_template.txt file")
	}
}

// MsSQLInit checks connection (ping) and sets, e.g., maximum amount of concurrent connection
func MsSQLInit(connectionName string, ConnMaxLifetimeHours int, MaxIdleConns int, MaxOpenConns int)(*sql.DB, context.Context){ 
    getEnvVars()
   
    var err error
var dbServer, dbDatabase, user, password string
	// Create connection string for source database
	dbServer = os.Getenv(connectionName + "_INSTANCE")
	dbDatabase = os.Getenv(connectionName + "_DATABASE")
	
	val, ok := os.LookupEnv(connectionName + "_USER")
		if !ok || val == "" {
		user = os.Getenv("FHIRHOSE_USER")
	} else {
		user = os.Getenv(connectionName + "_USER")
	}
	
	val, ok = os.LookupEnv(connectionName + "_PASS")
		if !ok || val == "" {
		password = os.Getenv("FHIRHOSE_PASS")
	} else {
		password = os.Getenv(connectionName + "_PASS")
	}
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s", dbServer, user, password, dbDatabase)
	
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or another initialization error.
		log.Fatal("unable to use data source name", err)
	}
	//defer db.Close()
	ctx := context.Background()
    // Ping database to see if it's still alive.
    // Important for handling network issues and long queries.
    err = db.PingContext(ctx)
    if err != nil {
        log.Fatal("Error pinging database: " + err.Error())
    }

	db.SetConnMaxLifetime(time.Duration(ConnMaxLifetimeHours) * time.Hour)
	db.SetMaxIdleConns(MaxIdleConns)
	db.SetMaxOpenConns(MaxOpenConns)

    // Run query and scan for result
	var result string
    err = db.QueryRowContext(ctx, "SELECT @@version").Scan(&result)
    if err != nil {
        log.Fatal("Scan failed:", err.Error())
    }
	fmt.Printf("%s: %s\n", connectionName, result)

	//initialize database in dataplatform (if it doesn't exists)
	if connectionName == "FHIRHOSE"{
		_,err = db.ExecContext(ctx, `
	
		/*
		DROP TABLE lsnMax
		DROP TABLE fhirJson
		DROP TABLE fhirIndex
		*/
		IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(N'lsnMax'))
		BEGIN
		CREATE TABLE lsnMax (
			id					BIGINT IDENTITY(1,1)	NOT NULL,	-- Unieke sleutel
			sourceTable 		VARCHAR(300)			NOT NULL,	-- bevat schema.table
			[lsn]		        BINARY(10)              NOT NULL,		
			[LogDate] 			DATETIME2				NULL DEFAULT (getdate()),		-- Wijzigingsdatum
		)
		END
	
		IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE [name] = 'idx_lsnMax_sourceTable' AND object_id = OBJECT_ID('lsnMax'))
		CREATE CLUSTERED INDEX [idx_lsnMax_sourceTable] ON [lsnMax] ([sourceTable] ASC)
	
		IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(N'fhirJson'))
		BEGIN
		CREATE TABLE fhirJson (
			versionId					BIGINT IDENTITY(1,1)	NOT NULL,	-- Unieke sleutel
			resourceId 		CHAR(64)			NOT NULL,	-- bevat schema.table
			[JSON]		        nvarchar(max)              NOT NULL,		
			[LogDate] 			DATETIME2				NULL DEFAULT (getdate()),		-- Wijzigingsdatum
		)
		END
	
		IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(N'fhirIndex'))
		BEGIN
		CREATE TABLE fhirIndex (
			resourceId 		CHAR(64)			NOT NULL,	-- bevat schema.table
			[item]		        nvarchar(max)              NOT NULL,		
			[LogDate] 			DATETIME2				NULL DEFAULT (getdate()),		-- Wijzigingsdatum
		)
		END
		`)
		if err != nil {
			log.Fatal(err)
		}
	}

	
return db, ctx
}