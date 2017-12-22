package main

import (
	"github.com/alexflint/go-arg"
)

type Options struct {
	Host     string `help:"mysql host"`
	Port     int    `help:"mysql port"`
	User     string `arg:"required" help:"mysql user"`
	Password string `arg:"required" help:"mysql password"`
	Database string `arg:"required" help:"database name for import data"`
	Table    string `arg:"required" help:"database table name for import data "`
	File     string `arg:"required" help:"data line file"`
	Fields   string `arg:"required" help:"data filed name "`
	Separate string `help:"separate line for field "`
	Worker   int    `help:"worker for import task"`
}

func main() {
	var options Options
	options.Port = 3306
	options.Separate = "\t"
	options.Worker = 10
	arg.MustParse(&options)

	Import(&options)
}
