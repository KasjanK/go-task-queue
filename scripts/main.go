package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

func main() {
	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			payload := map[string]any{
				"type": "email",
				"payload": map[string]any{
					"to": "test@example.com",
				},
			}

			jsonData, err := json.Marshal(payload)
			if err != nil {
				fmt.Printf("could not marshal data: %v", err)
			}

			resp, err := http.Post("http://localhost:8080/jobs", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				defer resp.Body.Close()
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			payload := map[string]any{
				"type": "resizer",
				"payload": map[string]any{
					"size": "1MB",
				},
			}

			jsonData, err := json.Marshal(payload)
			if err != nil {
				fmt.Printf("could not marshal data: %v", err)
			}

			resp, err := http.Post("http://localhost:8080/jobs", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				defer resp.Body.Close()
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			payload := map[string]any{
				"type": "fail",
				"payload": map[string]any{
					"size": "1MB",
				},
			}

			jsonData, err := json.Marshal(payload)
			if err != nil {
				fmt.Printf("could not marshal data: %v", err)
			}

			resp, err := http.Post("http://localhost:8080/jobs", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				defer resp.Body.Close()
			}
		}()
	}

	wg.Wait()
	fmt.Println("All 1000 jobs have been sent")
}
