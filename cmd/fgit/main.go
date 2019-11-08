package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const baseURL = "http://localhost:6060"

func main() {
	commands := map[string]func(context.Context, []string){
		"status": func(ctx context.Context, args []string) {
			resp, err := http.Get(baseURL + "/status/")
			if err != nil {
				log.Printf("Unable to read status: %s", err)
				return
			}

			io.Copy(os.Stdout, resp.Body)
		},
		"log": func(ctx context.Context, args []string) {
			resp, err := http.Get(baseURL + "/commits/")
			if err != nil {
				log.Printf("Unable to read log: %s", err)
				return
			}

			var commits []string
			decoder := json.NewDecoder(resp.Body)
			if err := decoder.Decode(&commits); err != nil {
				log.Printf("Unable to decode list of commits: %s", err)
				return
			}

			for _, c := range commits {
				fmt.Println(c)
			}
		},
		"checkout": func(ctx context.Context, args []string) {
			if len(args) != 1 {
				log.Fatal("Required exactly one argument for checkout")
			}
			start := time.Now()
			resp, err := http.Get(baseURL + "/checkout/" + args[0])
			if err != nil {
				log.Printf("Unable to call checkout: %s", err)
				return
			}
			fmt.Printf("Checkout to %s complete in %s\n", args[0], time.Since(start))
			_ = resp.Body.Close()
		},
		"fetch": func(ctx context.Context, args []string) {
			start := time.Now()
			resp, err := http.Get(baseURL + "/fetch/")
			if err != nil {
				log.Printf("Unable to call fetch: %s", err)
				return
			}
			fmt.Printf("Fetch complete in %s\n", time.Since(start))
			_ = resp.Body.Close()
		},
	}

	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	args := flag.Args()

	if len(args) == 0 || commands[args[0]] == nil {
		var validCmds []string
		for k := range commands {
			validCmds = append(validCmds, k)
		}
		log.Fatalf("Need to provide a subcommand: %s", validCmds)
	}

	commands[args[0]](context.Background(), args[1:])
}
