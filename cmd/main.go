package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/sebnyberg/gobloom"
	"github.com/sebnyberg/protoio"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func printHelp() {
	fmt.Println("go run main.go [gen-file]")
}

func main() {
	if len(os.Args) != 2 {
		printHelp()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "gen-file":
		genFile()
	default:
		printHelp()
		os.Exit(1)
	}
}

func genFile() {
	f, err := os.OpenFile("out.ldproto", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	check(err)
	bufw := bufio.NewWriter(f)
	outputWriter := protoio.NewWriter(bufw)
	defer func() {
		check(outputWriter.Close())
	}()

	for i := 0; i < 1e7; i++ {
		p := &gobloom.Prediction{
			Ts:           timestamppb.New(time.Now()),
			LocationId:   ksuid.New().String(),
			CapabilityId: ksuid.New().String(),
			Value:        rand.Float32(),
			Category:     int32(rand.Intn(6)),
		}
		check(outputWriter.WriteMsg(p))
		if i%1e5 == 0 {
			fmt.Printf("written %d messages\n", i)
		}
	}
	fmt.Println("done!")
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
