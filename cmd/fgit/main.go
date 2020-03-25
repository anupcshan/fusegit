package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/anupcshan/fusegit/fg_proto"
	"github.com/anupcshan/fusegit/fusegit"
	"google.golang.org/grpc"
)

const baseURL = "http://localhost:6060"

func locateCtrlPath(originPath string) (string, error) {
	if originPath == "" {
		var err error
		originPath, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}

	for {
		fullPath := path.Join(originPath, fusegit.CtlFile)
		if b, err := ioutil.ReadFile(fullPath); err == nil {
			return string(b), nil
		}

		if originPath == "" || originPath == "/" {
			return "", fmt.Errorf("Not a fusegit mount (or any of the parent directories)")
		}

		originPath = filepath.Dir(originPath)
	}
}

func UnixDialer(addr string, t time.Duration) (net.Conn, error) {
	unix_addr, err := net.ResolveUnixAddr("unix", addr)
	conn, err := net.DialUnix("unix", nil, unix_addr)
	return conn, err
}

func main() {
	flag.Parse()

	baseURL, err := locateCtrlPath("")
	if err != nil {
		log.Fatal(err)
	}

	conn, err := grpc.Dial(baseURL, grpc.WithInsecure(), grpc.WithDialer(UnixDialer))
	if err != nil {
		log.Fatal(err)
	}

	client := fg_proto.NewFusegitClient(conn)

	commands := map[string]func(context.Context, []string){
		"status": func(ctx context.Context, args []string) {
			resp, err := client.Status(ctx, &fg_proto.StatusRequest{})
			if err != nil {
				log.Printf("Unable to read status: %s", err)
				return
			}

			fmt.Println(resp.GetRevisionHash())
		},
		"log": func(ctx context.Context, args []string) {
			resp, err := client.Log(ctx, &fg_proto.LogRequest{})
			if err != nil {
				log.Printf("Unable to read log: %s", err)
				return
			}

			for _, c := range resp.GetRevisionHashes() {
				fmt.Println(c)
			}
		},
		"checkout": func(ctx context.Context, args []string) {
			if len(args) != 1 {
				log.Fatal("Required exactly one argument for checkout")
			}
			start := time.Now()
			_, err := client.Checkout(ctx, &fg_proto.CheckoutRequest{
				RevisionHash: args[0],
			})

			if err != nil {
				log.Printf("Unable to call checkout: %s", err)
				return
			}
			fmt.Printf("Checkout to %s complete in %s\n", args[0], time.Since(start))
		},
		"fetch": func(ctx context.Context, args []string) {
			start := time.Now()
			_, err := client.Fetch(ctx, &fg_proto.FetchRequest{})
			if err != nil {
				log.Printf("Unable to call fetch: %s", err)
				return
			}
			fmt.Printf("Fetch complete in %s\n", time.Since(start))
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
