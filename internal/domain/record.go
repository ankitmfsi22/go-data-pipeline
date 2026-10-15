package domain

type Record struct {
	Source string         `json:"source"`
	Line   int            `json:"line"`
	Fields map[string]any `json:"fields"`
}
