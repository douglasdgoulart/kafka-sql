package main

import (
	"github.com/douglasdgoulart/kafka-sql/internal/app"
	"github.com/douglasdgoulart/kafka-sql/internal/util"
)

func main() {
	config := util.NewConfiguration()
	app := app.NewApp(config)
	app.Run()
}
