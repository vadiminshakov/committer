package main

import (
	"fmt"
	"github.com/vadiminshakov/committer/client"
	"github.com/vadiminshakov/committer/config"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/server"
)

func main() {
	conf := config.Get()
	s := server.NewCommitServer(conf.Nodeaddr)
	if conf.Role == "coordinator" {
		for _, node := range conf.Followers {
			cli, err := client.New(node)
			if err != nil {
				panic("failed to create client")
			}
			response, err := cli.Propose(&pb.ProposeRequest{})
			if err != nil {
				panic(err)
			}
			fmt.Println(response.Type)
		}
	} else {
		server.Run(s)
	}
}
