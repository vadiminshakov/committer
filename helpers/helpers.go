package helpers

// Includes checks that the 'arr' includes 'value'
func Includes(arr []string, value string) bool {
	for i := range arr {
		if arr[i] == value {
			return true
		}
	}
	return false
}
