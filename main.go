package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/houqp/sqlvet/pkg/vet"
)

func main() {
	singlechecker.Main(vet.Analyzer)
}
