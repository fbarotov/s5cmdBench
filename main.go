package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gonum.org/v1/gonum/stat"
)

func main() {
	const (
		folder = "./blob"

		bucket1 = "s3://s5cmd-test-east"
		bucket2 = "s3://s5cmd-test-west"

		numOfIter = 10

		nano = 1e+9
	)

	rand.Seed(time.Now().UTC().UnixNano())

	durationS5cmdV100, durationS5cmd, err := compareUploadSpeed(
		numOfIter,
		folder,
		bucket1,
		bucket2,
	)
	if err != nil {
		fmt.Println(err)
	}

	x1 := make([]float64, numOfIter)
	x2 := make([]float64, numOfIter)

	for i := 0; i < numOfIter; i++ {
		x1[i] = float64(durationS5cmdV100[i]) / nano
		x2[i] = float64(durationS5cmd[i]) / nano
	}

	mean1, std1 := stat.MeanStdDev(x1, nil)
	mean2, std2 := stat.MeanStdDev(x2, nil)

	fmt.Println("\n", "Uploading 1100 files in batch mode, where each file is 10.1 KB")
	fmt.Println("number of iterations", numOfIter)

	stat1 := fmt.Sprintf(`s5cmd version 1.0.0 file upload speed (single region):
							mean: %v sec., 
							standard deviation: %v
						`, mean1, std1)

	stat2 := fmt.Sprintf(`s5cmd after adding cross-region transfer, file upload speed (two regions):
							mean: %v sec.,
							standard deviation: %v
						`, mean2, std2)

	fmt.Printf("%s\n%s\n", stat1, stat2)
}

func compareUploadSpeed(
	numOfIter int,
	folder string,
	bucket1 string,
	bucket2 string,
) ([]time.Duration, []time.Duration, error) {
	const (
		commands1 = "commands.txt"
		commands2 = "commandsWithTwoRegion.txt"

		exec1 = "./s5cmd_v1.0.0.exe"
		exec2 = "./s5cmd.exe"
	)
	var (
		durations1 []time.Duration
		durations2 []time.Duration
	)

	for i := 0; i < numOfIter; i++ {
		pre1 := "/" + randomString(10) + "/"
		pre2 := "/" + randomString(10) + "/"

		{
			err := newCommands(commands1, folder, func() string {
				return strings.Join(
					[]string{
						"cp %s",
						fmt.Sprintf("%s\n", bucket1+pre1),
					}, " ")
			})
			if err != nil {
				return nil, nil, err
			}

			duration, err := measureRuntime(exec1, "run", commands1)
			if err != nil {
				return nil, nil, err
			}

			durations1 = append(durations1, duration)
		}

		{
			err := newCommands(commands2, folder, func() string {
				bucket := [...]string{bucket1, bucket2}[rand.Intn(2)]

				return strings.Join(
					[]string{
						"cp %s",
						fmt.Sprintf("%s\n", bucket+pre2),
					}, " ")
			})
			if err != nil {
				return nil, nil, err
			}

			duration, err := measureRuntime(exec2, "run", commands2)
			if err != nil {
				return nil, nil, err
			}

			durations2 = append(durations2, duration)
		}
	}
	return durations1, durations2, nil
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

func randomString(length int) string {
	const symbols = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890"

	b := make([]byte, length)

	for i := 0; i < length; i++ {
		b[i] = symbols[rand.Intn(len(symbols))]
	}
	return string(b)
}
