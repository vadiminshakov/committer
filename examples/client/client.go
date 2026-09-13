// This example writes and reads five keys through the public Go client.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
)

func main() {
	addr := flag.String("addr", "localhost:3000", "coordinator address")
	timeout := flag.Duration("timeout", 5*time.Second, "deadline for each request")
	key := flag.String("key", "somekey", "key prefix")
	value := flag.String("value", "somevalue", "value prefix")
	flag.Parse()
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "timeout must be positive")
		os.Exit(1)
	}
	if err := run(*addr, *key, *value, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "Example failed:", err)
		os.Exit(1)
	}
}

func run(addr, key, value string, timeout time.Duration) error {
	cli, err := client.NewClientAPI(addr)
	if err != nil {
		return err
	}
	defer func() {
		_ = cli.Close()
	}()
	for i := 0; i < 5; i++ {
		k, v := key+strconv.Itoa(i), value+strconv.Itoa(i)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		resp, err := cli.Put(ctx, k, []byte(v))
		cancel()
		if err != nil {
			return fmt.Errorf("put %q: %w; check that coordinator and cohort are running at the configured addresses", k, err)
		}
		if resp.Type != pb.Type_ACK {
			return fmt.Errorf("transaction %d was rejected", resp.Index)
		}
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		result, err := cli.Get(ctx, k)
		cancel()
		if err != nil {
			return fmt.Errorf("get %q: %w", k, err)
		}
		fmt.Printf("got value for key '%s': %s\n", k, string(result.Value))
	}
	return nil
}
