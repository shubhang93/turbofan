package offman

type AckStatus int8

func (a AckStatus) MarshalText() (text []byte, err error) {
	return []byte(a.String()), nil
}

const (
	StatusNack AckStatus = iota
	StatusAck
	StatusCommitted
)

func (a AckStatus) String() string {
	switch a {
	case StatusAck:
		return "ACK"
	case StatusNack:
		return "NACK"
	case StatusCommitted:
		return "COMMIT_CHECKPOINT"
	default:
		return "UNKNOWN"
	}
}
