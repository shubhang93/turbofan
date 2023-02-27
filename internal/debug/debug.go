package debug

import (
	"log"
	"os"
)

func Log(format string, args ...any) {
	if value, ok := os.LookupEnv("TURBO_FAN_DEBUG"); ok && value == "1" {
		log.Printf(format, args...)
	}
}
