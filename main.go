package main

import (
	"fmt"
	"os"

	mcli "github.com/mitchellh/cli"
	"github.com/thisdrunkdane/msgreplay/command/example/amqp"
	"github.com/thisdrunkdane/msgreplay/command/example/api"
)

func main() {
	ui := &mcli.ConcurrentUi{
		Ui: &mcli.ColoredUi{
			ErrorColor: mcli.UiColorRed,
			WarnColor: mcli.UiColorYellow,
			Ui: &mcli.PrefixedUi{
				OutputPrefix: "[OUT ] ",
				InfoPrefix: "[INFO] ",
				ErrorPrefix: "[ERR ] ",
				WarnPrefix: "[WARN] ",
				Ui: &mcli.BasicUi{Writer: os.Stdout, ErrorWriter: os.Stderr},
			},
		},
	}
	cli := mcli.NewCLI("msgreplay", "N/A")
	cli.Args = os.Args[1:]
	cli.Commands = map[string]mcli.CommandFactory{
		"example amqp": func() (mcli.Command, error) {
			return amqp.New(ui), nil
		},
		"example api": func() (mcli.Command, error) {
			return api.New(ui), nil
		},
	}

	code, err := cli.Run()
	if err != nil {
		ui.Error(fmt.Sprintf("%v\n", err))
	}

	os.Exit(code)
}
