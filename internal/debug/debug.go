package debug

import (
	"log"
	"os"
)

var debugVal, present = os.LookupEnv("TURBO_FAN_DEBUG")

func Log(format string, args ...any) {
	if present && debugVal == "1" {
		log.Printf(format, args...)
	}
}
