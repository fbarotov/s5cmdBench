package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	const (
		folder = "./blob"

		commands           = "commands.txt"
		commandsWithRegion = "commandsWithRegion.txt"

		defaultBucket = 0
	)

	var (
		buckets = []string{"s3://bucket-faru-1", "s3://bucket-faru-2"}
		regions = []string{"us-east-1", "us-east-2"}

		durations []time.Duration
	)

	{
		err := newCommands(commands, folder, func() string {
			return strings.Join(
				[]string{
					"cp %s",
					fmt.Sprintf("%s\n", buckets[defaultBucket]),
				}, " ")
		})
		logFatIf(err)

		duration, err := measureRuntime("./s5cmd_v1.0.0.exe", "run", commands)
		logFatIf(err)

		durations = append(durations, duration)
	}

	// delete files after uploading
	_, err := measureRuntime("./s5cmd_v1.0.0.exe", "rm", buckets[defaultBucket]+"/*")
	logFatIf(err)

	{
		err := newCommands(commandsWithRegion, folder, func() string {
			rnd := rand.Intn(2)
			return strings.Join(
				[]string{
					fmt.Sprintf("cp --region=%s", regions[rnd]),
					"%s",
					fmt.Sprintf("%s\n", buckets[rnd]),
				}, " ")
		})
		logFatIf(err)

		duration, err := measureRuntime("./s5cmd.exe", "run", commandsWithRegion)
		logFatIf(err)

		durations = append(durations, duration)
	}

	// delete files after uploading
	const regionVar = "AWS_DEFAULT_REGION"
	for i, bucket := range buckets {
		err := os.Setenv(regionVar, regions[i])
		logFatIf(err)

		_, err = measureRuntime("./s5cmd.exe", "rm", bucket+"/*")
		logFatIf(err)

		err = os.Unsetenv(regionVar)
		logFatIf(err)
	}

	fmt.Println(durations)
}

// newCommands creates commands file to be used for s5cmd batch mode;
// the given folder is iterated and for each file in the folder,
// separate copy command is created.
func newCommands(fname, srcFolder string, format func() string) error {
	f, err := os.Create(fname)
	if err != nil {
		return err
	}

	err = filepath.Walk(srcFolder, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		path = filepath.ToSlash(path)

		_, err = fmt.Fprintf(f, format(), path)
		return err
	})

	if err != nil {
		return err
	}

	return f.Close()
}

func logFatIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// measureRuntime measures execution time of the executable
// given the flag arguments.
func measureRuntime(programPath string, args ...string) (time.Duration, error) {
	now := time.Now()
	s5cmd := &exec.Cmd{
		Path:   programPath,
		Args:   append([]string{programPath}, args...),
		Stdout: os.Stdout,
		Stderr: os.Stdout,
	}

	if err := s5cmd.Run(); err != nil {
		return 0, err
	}

	return time.Now().Sub(now), nil
}
