package eventstorming

type Event struct {
	EventID   string `json:"event_id,omitempty"   bson:"_id,omitempty"`
	EventName string `json:"event_name,omitempty" bson:"event_name,omitempty"`
	EventType string `json:"event_type,omitempty" bson:"event_type,omitempty"`
	DataID    string `json:"data_id,omitempty"    bson:"data_id,omitempty"`
	DataName  string `json:"data_name,omitempty"  bson:"data_name,omitempty"`
	Data      string `json:"data,omitempty"       bson:"data,omitempty"`
	CreateBy  string `json:"create_by,omitempty"  bson:"create_by,omitempty"`
	TimeStamp string `json:"timestamp,omitempty"  bson:"timestamp,omitempty"`
}
