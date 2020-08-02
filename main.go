package main

import (
	"fmt"
	"github.com/vadiminshakov/committer/client"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/server"
	"os"
)

func main() {
	s := server.NewCommitServer()
	if len(os.Args) > 1 && os.Args[1] == "coordinator" {
		server.Run(s)
	} else {
		cli, err := client.New("localhost", "3050")
		if err != nil {
			panic("failed to create client")
		}
		response, err := cli.Propose(&pb.ProposeRequest{})
		if err != nil {
			panic(err)
		}
		fmt.Println(response.Type)
	}
}
