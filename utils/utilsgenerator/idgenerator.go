package utilsgenerator

import (
	"gopkg.in/mgo.v2/bson"
)

func NewID() string {
	return bson.NewObjectId().Hex()
}
