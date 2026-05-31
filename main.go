package main

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func main() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	app := cli.NewApp()
	app.Name = "s3-cache"
	app.Usage = "Transparent S3 proxy with multi-range parallel fetch and abort-on-slow"
	app.Version = "0.0.1"
	configure(app)
	if err := app.Run(os.Args); err != nil {
		log.WithError(err).Fatal("failed to serve application")
	}
}
