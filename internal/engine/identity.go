package engine

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
)

func newTableID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// Assign a durable identity once to catalogs created before table IDs existed.
func migrateTableIDs(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query("SELECT namespace,name,definition FROM main.emulator_catalog")
	if err != nil {
		return err
	}
	type update struct{ ns, name, definition string }
	updates := []update{}
	for rows.Next() {
		var ns, name, raw string
		if err = rows.Scan(&ns, &name, &raw); err != nil {
			rows.Close()
			return err
		}
		var t Table
		if err = json.Unmarshal([]byte(raw), &t); err != nil {
			rows.Close()
			return err
		}
		if t.ID != "" {
			continue
		}
		if t.ID, err = newTableID(); err != nil {
			rows.Close()
			return err
		}
		b, er := json.Marshal(t)
		if er != nil {
			rows.Close()
			return er
		}
		updates = append(updates, update{ns, name, string(b)})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, u := range updates {
		if _, err = tx.Exec("UPDATE main.emulator_catalog SET definition=? WHERE namespace=? AND name=?", u.definition, u.ns, u.name); err != nil {
			return err
		}
	}
	return tx.Commit()
}
