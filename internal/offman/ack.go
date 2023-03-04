package offman

type StatusACK int8

func (a StatusACK) MarshalText() (text []byte, err error) {
	return []byte(a.String()), nil
}

const (
	StatusNack StatusACK = iota
	StatusAck
	StatusCommitted
)

func (a StatusACK) String() string {
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
