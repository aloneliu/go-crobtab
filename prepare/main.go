package main

import (
	"os/exec"
	"fmt"
)

func main() {
	command := exec.Command("/bin/bash", "-c", "echo1;echo2;")
	run := command.Run()

	fmt.Println(run)
}
