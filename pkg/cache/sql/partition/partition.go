/*
Package partition represents listing parameters. They can be used to specify which namespaces a caller would like included
in a response, or which specific objects they are looking for.
*/
package partition

import (
	"k8s.io/apimachinery/pkg/util/sets"
)

// Partition represents filtering of a request's results by namespace or a list of resource names
type Partition struct {
	Namespace   string
	All         bool
	Passthrough bool
	Names       sets.String
}
