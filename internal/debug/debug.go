package debug

import (
	"log"
	"os"
)

func Log(format string, args ...any) {
	if _, ok := os.LookupEnv("DEBUG"); ok {
		log.Printf(format, args...)
	}
}
