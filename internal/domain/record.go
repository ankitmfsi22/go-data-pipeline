package domain

type Record struct {
	Source string
	Line   int
	Fields map[string]any
}
