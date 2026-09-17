// Package contract embeds the exact public OpenAPI document in the binary.
package contract

import _ "embed"

//go:embed openapi.yaml
var OpenAPI []byte
