package main

import (
	"context"

	"github.com/douglasdgoulart/kafka-sql/internal/app"
	"github.com/douglasdgoulart/kafka-sql/internal/util"
)

func main() {
	config := util.NewConfiguration()
	ctx := context.Background()

	app := app.NewApp(config, ctx)
	app.Run()
}
