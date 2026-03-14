package types

// ValidMode reports whether m is an allowed mode value.
func ValidMode(m string) bool {
	switch m {
	case "deterministic", "probabilistic", "replay":
		return true
	default:
		return false
	}
}
