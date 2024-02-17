package util

import (
	"bytes"
	"encoding/json"
	"math/rand/v2"
	"strconv"
)

func GenerateRandomJSON(approxSize int) (string, error) {
	var jsonBuffer bytes.Buffer
	jsonBuffer.WriteString("{")
	currentSize := 2
	approxSize = int(float64(approxSize) * 1.2)

	for currentSize < approxSize {
		key := "key" + strconv.Itoa(rand.IntN(10000))
		value := rand.IntN(10000)
		keyValueJSON, err := json.Marshal(map[string]int{key: value})
		if err != nil {
			return "", err
		}
		keyValueStr := string(keyValueJSON[1 : len(keyValueJSON)-1])
		if currentSize+len(keyValueStr)+1 > approxSize {
			break
		}
		if currentSize > 2 {
			jsonBuffer.WriteString(",")
		}
		jsonBuffer.WriteString(keyValueStr)
		currentSize += len(keyValueStr) + 1
	}

	jsonBuffer.WriteString("}")

	return jsonBuffer.String(), nil
}
