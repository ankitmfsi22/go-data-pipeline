package domain

type AggResult struct {
	Op      string             `json:"op"`
	Field   string             `json:"field,omitempty"`
	GroupBy string             `json:"group_by,omitempty"`
	Values  map[string]float64 `json:"values"`
}
