package utilsmongo

import (
	"errors"
	"os"

	mgo "gopkg.in/mgo.v2"
)

const (
	MONGO_DB_URL      = "MONGO_DB_URL"
	MONGO_DB_USERNAME = "MONGO_USERNAME"
	MONGO_DB_PASSWORD = "MONGO_PASSWORD"
	MONGO_DB_NAME     = "MONGO_DB_NAME"
)

const (
	MONGO_EVENTSTORE_URL      = "MONGO_EVENTSTORE_URL"
	MONGO_EVENTSTORE_NAME     = "MONGO_EVENTSTORE_NAME"
	MONGO_EVENTSTORE_USERNAME = "MONGO_EVENTSTORE_USERNAME"
	MONGO_EVENTSTORE_PASSWORD = "MONGO_EVENTSTORE_PASSWORD"
)

func MongoDBLogin() (*mgo.Database, error) {
	return loginDB(os.Getenv(MONGO_DB_URL), os.Getenv(MONGO_DB_NAME), os.Getenv(MONGO_DB_USERNAME), os.Getenv(MONGO_DB_PASSWORD))
}

func MongoEventStoreLogin() (*mgo.Database, error) {
	return loginDB(os.Getenv(MONGO_EVENTSTORE_URL), os.Getenv(MONGO_EVENTSTORE_NAME), os.Getenv(MONGO_EVENTSTORE_USERNAME), os.Getenv(MONGO_EVENTSTORE_PASSWORD))
}

func loginDB(url, databaseName, username, password string) (*mgo.Database, error) {
	if url == "" {
		return nil, errors.New("missing url mongo")
	}
	if databaseName == "" {
		return nil, errors.New("missing mongo database name")
	}
	session, err := mgo.Dial(url)
	if err != nil {
		return nil, err
	}
	if username != "" && password != "" {
		if err := session.DB(databaseName).Login(username, password); err != nil {
			return nil, err
		}
	}
	return session.DB(databaseName), nil
}
