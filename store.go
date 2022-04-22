package raft

// Store is used to provide stable storage
// of key configurations to ensure safety.
type Store interface {
	Set(key []byte, val []byte) error
	// Get returns the value for key, or an empty byte slice if key was not found.
	Get(key []byte) ([]byte, error)

	SetInt(key []byte, val int) error
	// GetInt returns the int value for key, or 0 if key was not found.
	GetInt(key []byte) (int, error)
}
