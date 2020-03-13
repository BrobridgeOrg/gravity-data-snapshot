package data_snapshot

type DatabaseManager struct {
	databases map[string]*Database
}

func CreateDatabaseManager() *DatabaseManager {
	return &DatabaseManager{
		databases: make(map[string]*Database),
	}
}

func (dm *DatabaseManager) GetDatabase(dbname string) *Database {

	if db, ok := dm.databases[dbname]; ok {
		return db
	}

	db := OpenDatabase(dbname)
	if db == nil {
		return nil

	}

	dm.databases[dbname] = db

	return db
}
