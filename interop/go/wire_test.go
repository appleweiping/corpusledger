package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func fixture(t *testing.T, text string) map[string]any {
	t.Helper()
	request := map[string]any{"format": "corpusledger.processor-request.v1", "operation_id": "op", "step_id": "step", "processor": description(strings.Repeat("a", 64)), "input_digest": strings.Repeat("b", 64), "document": map[string]any{"format": "corpusledger.annotations.v1", "id": "doc", "text": text, "text_sha256": digest([]byte(text)), "offset_unit": "unicode_codepoint", "types": []any{}, "annotations": []any{}}}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	value, err := decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return value.(map[string]any)
}

func TestStrictJSON(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `[NaN]`, `[1e400]`, `["\ud800"]`, `["\udfff"]`, `["\ud800x"]`, `{} []`, strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65), "[" + strings.Repeat("1", 4301) + "]"} {
		if _, err := decode([]byte(raw)); err == nil {
			t.Fatalf("accepted malformed JSON of length %d", len(raw))
		}
	}
	if _, err := decode([]byte{'[', '"', 0xff, '"', ']'}); err == nil {
		t.Fatal("accepted invalid UTF8")
	}
	value, err := decode([]byte(`["\ud83d\ude00",123456789012345678901234567890]`))
	if err != nil {
		t.Fatal(err)
	}
	if value.([]any)[0] != "😀" || string(value.([]any)[1].(json.Number)) != "123456789012345678901234567890" {
		t.Fatal("Unicode/integer changed")
	}
}

func TestCodepointWhitespaceTokens(t *testing.T) {
	r := fixture(t, "A😀 e\u0301\r\n\t終\u00a0Z")
	result, err := processRequest(r, description(strings.Repeat("a", 64)))
	if err != nil {
		t.Fatal(err)
	}
	rows := result["annotations"].([]any)
	if len(rows) != 4 {
		t.Fatal("wrong token count")
	}
	for i, span := range [][2]int{{0, 2}, {3, 5}, {8, 9}, {10, 11}} {
		a := rows[i].(map[string]any)
		if a["start"] != span[0] || a["end"] != span[1] {
			t.Fatal("wrong codepoint offset")
		}
	}
	if result["input_digest"] != r["input_digest"] {
		t.Fatal("opaque input identity changed")
	}
	empty, err := processRequest(fixture(t, ""), description(strings.Repeat("a", 64)))
	if err != nil || len(empty["annotations"].([]any)) != 0 {
		t.Fatal("empty document failed")
	}
}

func TestIdentityAndClosedDocument(t *testing.T) {
	for _, mutate := range []func(map[string]any){
		func(r map[string]any) { r["extra"] = true },
		func(r map[string]any) { r["document"].(map[string]any)["text_sha256"] = strings.Repeat("c", 64) },
		func(r map[string]any) { r["document"].(map[string]any)["offset_unit"] = "utf16" },
		func(r map[string]any) { r["document"].(map[string]any)["types"] = []any{tokenSchema()} },
		func(r map[string]any) { r["processor"].(map[string]any)["version"] = "2" },
	} {
		r := fixture(t, "x")
		mutate(r)
		if _, err := processRequest(r, description(strings.Repeat("a", 64))); err == nil {
			t.Fatal("accepted modified contract")
		}
	}
}
