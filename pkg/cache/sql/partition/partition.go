/*
Package partition represents listing parameters. They can be used to specify which namespaces a caller would like included
in a response, or which specific objects they are looking for.
*/
package partition

import (
	"k8s.io/apimachinery/pkg/util/sets"
)

// Partition represents filtering of a request's results by namespace or a list of resource names.
// If All is false and Names is empty, that indicates an empty search space that result in an empty list
type Partition struct {
	Namespace   string
	All         bool
	Passthrough bool
	Names       sets.Set[string]
}
