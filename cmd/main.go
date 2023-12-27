package main;

import (
	"fmt"
	"flag"
	mem "github.com/mjacob1002/Distributed-Filesystem/pkg/membership"
)

func main(){
	var myNodeNumber int64 = -1
	var membershipPort string = "blah"
	flag.Int64Var(&myNodeNumber, "logical_node", -1, "the logical node number for this node in the system")
	flag.StringVar(&membershipPort, "mem_port", "shit", "the port to be used for membership list protocol");
	flag.StringVar(&mem.MyIntroducer, "introducer", "", "the hostname:port of the introducer to the system");
	flag.Parse()
	mem.InitializeMembership(myNodeNumber, membershipPort);
	for {
		var input1 string;
		_ , err := fmt.Scanln(&input1);
		if err != nil {
			fmt.Printf("error: %v\n", err);
		}
		if input1 == "members"{
			members := mem.GetMembers()
			for _, member := range(members) {
				fmt.Printf("%s\n", member);
			}
		}
	}
}
