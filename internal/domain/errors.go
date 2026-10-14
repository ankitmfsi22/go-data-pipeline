package domain

import "fmt"

type RecordError struct {
	Stage  string
	Record Record
	Reason string
}

func (e RecordError) Error() string {
	return fmt.Sprintf("%s: %s line %d: %s", e.Stage, e.Record.Source, e.Record.Line, e.Reason)
}
