package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/service/ec2"
	_ "github.com/mattn/go-sqlite3"
)

var createOfferingTableSQL = `
	CREATE TABLE IF NOT EXISTS offering (
		Id                          TEXT NOT NULL PRIMARY KEY,
		InstanceType                TEXT NOT NULL,
		OfferingType                TEXT NOT NULL,
		AvailabilityZone            TEXT NOT NULL,
		InstanceTenancy             TEXT NOT NULL,
		CurrencyCode                TEXT NOT NULL,
		PricingDetails_Count1       INTEGER,
		PricingDetails_Price1       INTEGER,
		PricingDetails_Count2       INTEGER,
		PricingDetails_Price2       INTEGER,
		PricingDetails_Count3       INTEGER,
		PricingDetails_Price3       INTEGER,
		PricingDetails_Count4       INTEGER,
		PricingDetails_Price4       INTEGER,
		PricingDetails_Count5       INTEGER,
		PricingDetails_Price5       INTEGER,
		RecurringCharges_Amount1    INTEGER,
		RecurringCharges_Frequency1 INTEGER,
		RecurringCharges_Amount2    INTEGER,
		RecurringCharges_Frequency2 INTEGER,
		ProductDescription          TEXT NOT NULL,
		UsagePrice                  INTEGER NOT NULL,
		FixedPrice                  INTEGER NOT NULL,
		Duration                    INTEGER NOT NULL,
		Inserted                    DATETIME NOT NULL
	);
`

type database struct {
	*sql.DB
}

func initDatabase() (*database, error) {
	db, err := sql.Open("sqlite3", "./db.sqlite")
	if err != nil {
		return nil, err
	}

	if _, err = db.Exec(createOfferingTableSQL); err != nil {
		return nil, fmt.Errorf("Failed to create offering table: %v", err)
	}

	return &database{db}, nil
}

func (db *database) StoreOffering(offering *ec2.ReservedInstancesOffering) error {
	var fields = []string{
		"Id",
		"InstanceType",
		"OfferingType",
		"AvailabilityZone",
		"InstanceTenancy",
		"CurrencyCode",
		"ProductDescription",
		"UsagePrice",
		"FixedPrice",
		"Duration",
		"Inserted",
	}

	var values = []string{
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"?",
		"CURRENT_TIMESTAMP",
	}

	var bind = []interface{}{
		offering.ReservedInstancesOfferingId,
		offering.InstanceType,
		offering.OfferingType,
		offering.AvailabilityZone,
		offering.InstanceTenancy,
		offering.CurrencyCode,
		offering.ProductDescription,
		offering.UsagePrice,
		offering.FixedPrice,
		offering.Duration,
	}

	for idx, detail := range offering.PricingDetails {
		fields = append(fields, fmt.Sprintf("PricingDetails_Count%d", idx+1))
		values = append(values, "?")
		bind = append(bind, detail.Count)
		fields = append(fields, fmt.Sprintf("PricingDetails_Price%d", idx+1))
		values = append(values, "?")
		bind = append(bind, detail.Price)
	}

	for idx, charge := range offering.RecurringCharges {
		fields = append(fields, fmt.Sprintf("RecurringCharges_Amount%d", idx+1))
		values = append(values, "?")
		bind = append(bind, charge.Amount)
		fields = append(fields, fmt.Sprintf("RecurringCharges_Frequency%d", idx+1))
		values = append(values, "?")
		bind = append(bind, charge.Frequency)
	}

	sql := fmt.Sprintf(`INSERT OR IGNORE INTO offering(%s) VALUES(%s)`,
		strings.Join(fields, ","),
		strings.Join(values, ","),
	)

	stmt, err := db.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(bind...)
	if err != nil {
		log.Printf("Exec failed with offering %v", offering)
		return err
	}

	return nil
}

// func ReadItem(db *sql.DB) []TestItem {
// 	sql_readall := `
// 	SELECT Id, Name, Phone FROM items
// 	ORDER BY datetime(InsertedDatetime) DESC
// 	`

// 	rows, err := db.Query(sql_readall)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer rows.Close()

// 	var result []TestItem
// 	for rows.Next() {
// 		item := TestItem{}
// 		err2 := rows.Scan(&item.Id, &item.Name, &item.Phone)
// 		if err2 != nil {
// 			panic(err2)
// 		}
// 		result = append(result, item)
// 	}
// 	return result
// }
