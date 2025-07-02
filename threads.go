package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

func threads(){
	start := time.Now()
	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 50 * time.Second)
	defer cancel()

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request){
			time.Sleep(10 * time.Second)
			fmt.Fprintln(w, "Hello, world")
		}),
	)

	for range 10 {
		go func(ctx context.Context) {
			defer wg.Done()

			req, err := http.NewRequestWithContext(
				ctx, 
				"GET", 
				server.URL,
				nil,
			)
			if err != nil {
				panic(err)
			}

			resp, err := http.DefaultClient.Do(req)

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded){
					fmt.Println("timeout")
					return
				}
				panic(err)
			}

			defer resp.Body.Close()
		}(ctx)
	} 
	wg.Wait()
	
	fmt.Println(time.Since(start))
}