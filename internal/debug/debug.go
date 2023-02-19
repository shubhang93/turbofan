package debug

import (
	"log"
	"os"
)

func Log(format string, args ...any) {
	if _, ok := os.LookupEnv("OFFMAN_DEBUG"); ok {
		log.Printf(format, args...)
	}
}
