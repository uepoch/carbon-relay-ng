package change

type ActionType int

const (
	Add ActionType = iota
	Remove
)

type Change struct {
	Type    ActionType
	Address string
}
