package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/sebnyberg/gobloom"
	"github.com/sebnyberg/protoio"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func printHelp() {
	fmt.Println("go run main.go [gen-file|find-dup-with-map|find-dup-with-filter]")
}

func main() {
	if len(os.Args) != 2 {
		printHelp()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "gen-file":
		genFile()
	case "find-dup-with-map":
		findDuplicatesWithMap()
	case "find-dup-with-filter":
		findDuplicatesWithFilter()
	default:
		printHelp()
		os.Exit(1)
	}
}

// Generate a file with a lot of protobuf messages
func genFile() {
	f, err := os.OpenFile("out.ldproto", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	check(err)
	bufw := bufio.NewWriter(f)
	outputWriter := protoio.NewWriter(bufw)
	defer func() {
		bufw.Flush()
		f.Close()
	}()

	for i := 0; i < 1e7; i++ {
		// Create and write a new message (Prediction)
		p := &gobloom.Prediction{
			Ts:           timestamppb.New(time.Now()),
			LocationId:   ksuid.New().String(),
			CapabilityId: ksuid.New().String(),
			Value:        rand.Float32(),
			Category:     int32(rand.Intn(6)),
		}
		check(outputWriter.WriteMsg(p))

		// Print every ie6 messages
		if i%1e6 == 0 {
			fmt.Printf("written %d messages\n", i)
		}

		// Insert one in 1000 messages twice
		if rand.Intn(1000) == 1 {
			check(outputWriter.WriteMsg(p))
			i++
		}
	}
}

func findDuplicatesWithMap() {
	// Print elapsed time once we are finished
	defer timed(time.Now())

	// Open the file
	f, err := os.OpenFile("out.ldproto", os.O_RDONLY, 0644)
	check(err)
	bufr := bufio.NewReader(f)
	outputReader := protoio.NewReader(bufr)
	seen := make(map[string]bool, 3e7)

	// Count duplicates
	duplicates := 0

	var msg gobloom.Prediction
	for i := 0; ; i++ {
		// Read a message
		err := outputReader.ReadMsg(&msg)
		if err != nil {
			// Exit on EOF
			if err == io.EOF {
				break
			}
			log.Fatalf("failed to read message, err: %v\n", err)
		}

		// Check if the message key exists
		key := string(getKey(&msg))
		if _, exists := seen[key]; exists {
			duplicates++
		}

		// Set "seen" to true
		seen[key] = true

		if i%1e6 == 0 {
			fmt.Printf("read %d messages\n", i)
		}
	}

	printMemUsage()
	fmt.Println("done")
	fmt.Println("duplicates:", duplicates)
}

func findDuplicatesWithFilter() {
	// Print elapsed time once we are finished
	defer timed(time.Now())

	// Open the file
	f, err := os.OpenFile("out.ldproto", os.O_RDONLY, 0644)
	check(err)
	bufr := bufio.NewReader(f)
	outputReader := protoio.NewReader(bufr)
	filter := gobloom.NewFilter(1e7, 0.001)

	// Count duplicates
	likelyDuplicates := 0

	var msg gobloom.Prediction
	for i := 0; ; i++ {
		// Read a message
		err := outputReader.ReadMsg(&msg)
		if err != nil {
			// Exit on EOF
			if err == io.EOF {
				break
			}
			log.Fatalf("failed to read message, err: %v\n", err)
		}

		// If the filter returns true, this key has possibly been seen before
		// I.e. it is likely to be a duplicate
		if filter.TestAndAdd(getKey(&msg)) {
			likelyDuplicates++
		}

		if i%1e6 == 0 {
			fmt.Printf("read %d messages\n", i)
		}
	}
	printMemUsage()
	fmt.Println("done")
	fmt.Println("duplicates:", likelyDuplicates)
}

func getKey(p *gobloom.Prediction) []byte {
	h := fnv.New128a()
	h.Write([]byte(p.Ts.AsTime().Format(time.RFC3339)))
	h.Write([]byte(p.CapabilityId))
	h.Write([]byte(p.LocationId))
	h.Write([]byte(strconv.Itoa(int(p.Category))))
	id := make([]byte, 16)
	return h.Sum(id)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// printMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func timed(start time.Time) {
	fmt.Println("time elapsed", time.Since(start))
}
