package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxWire = 16 * 1024 * 1024

var invalid = errors.New("invalid request")
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var hashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var kinds = map[string]bool{"string": true, "integer": true, "number": true, "boolean": true, "object": true, "array": true, "reference": true, "references": true}

func digest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

// encoding/json replaces malformed UTF-8 and lone escaped surrogates. Reject
// those inputs before decoding so its replacement behavior cannot alter text.
func unicodeJSON(raw []byte) error {
	if !utf8.Valid(raw) {
		return invalid
	}
	inString := false
	for i := 0; i < len(raw); i++ {
		if raw[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return invalid
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return invalid
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return invalid
		}
		i += 4
		if n >= 0xDC00 && n <= 0xDFFF {
			return invalid
		}
		if n < 0xD800 || n > 0xDBFF {
			continue
		}
		if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return invalid
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xDC00 || low > 0xDFFF {
			return invalid
		}
		i += 6
	}
	return nil
}

func readValue(d *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, invalid
	}
	token, err := d.Token()
	if err != nil {
		return nil, invalid
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			result := map[string]any{}
			for d.More() {
				keyToken, err := d.Token()
				if err != nil {
					return nil, invalid
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, invalid
				}
				if _, exists := result[key]; exists {
					return nil, invalid
				}
				item, err := readValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				result[key] = item
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return nil, invalid
			}
			return result, nil
		case '[':
			result := []any{}
			for d.More() {
				item, err := readValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				result = append(result, item)
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return nil, invalid
			}
			return result, nil
		}
		return nil, invalid
	case json.Number:
		text := string(value)
		if strings.ContainsAny(text, ".eE") {
			f, err := strconv.ParseFloat(text, 64)
			if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
				return nil, invalid
			}
		} else if len(strings.TrimPrefix(text, "-")) > 4300 {
			return nil, invalid
		}
	}
	return token, nil
}

func decode(raw []byte) (any, error) {
	if len(raw) > maxWire || unicodeJSON(raw) != nil {
		return nil, invalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	value, err := readValue(d, 0)
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, invalid
	}
	return value, nil
}

func object(value any, keys ...string) (map[string]any, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return nil, invalid
	}
	if keys != nil {
		if len(m) != len(keys) {
			return nil, invalid
		}
		for _, key := range keys {
			if _, exists := m[key]; !exists {
				return nil, invalid
			}
		}
	}
	return m, nil
}

func name(value any) (string, error) {
	s, ok := value.(string)
	if !ok || strings.TrimFunc(s, func(r rune) bool { return space(r) || r >= 0x1C && r <= 0x1F }) == "" || utf8.RuneCountInString(s) > 256 {
		return "", invalid
	}
	return s, nil
}

func offset(value any, max int) (int, error) {
	n, ok := value.(json.Number)
	if !ok || strings.ContainsAny(string(n), ".eE") {
		return 0, invalid
	}
	i, err := strconv.Atoi(string(n))
	if err != nil || i < 0 || i > max {
		return 0, invalid
	}
	return i, nil
}

func field(kind string, target any) map[string]any {
	return map[string]any{"kind": kind, "required": true, "nullable": false, "target_type": target}
}

func tokenSchema() map[string]any {
	return map[string]any{"name": "token", "fields": map[string]any{"text": field("string", nil), "position": field("integer", nil)}}
}

func description(config string) map[string]any {
	return map[string]any{"format": "corpusledger.processor.v1", "name": "demo.go.tokens", "version": "1", "config_sha256": config, "requires": []any{}, "produces": []any{tokenSchema()}}
}

// Validate all existing layers, including unrelated features/references. Integers
// stay json.Number; they are never silently routed through binary64.
func document(value any) (map[string]any, map[string]map[string]any, error) {
	doc, err := object(value, "format", "id", "text", "text_sha256", "offset_unit", "types", "annotations")
	if err != nil || doc["format"] != "corpusledger.annotations.v1" || doc["offset_unit"] != "unicode_codepoint" {
		return nil, nil, invalid
	}
	if _, err = name(doc["id"]); err != nil {
		return nil, nil, err
	}
	text, ok := doc["text"].(string)
	length := utf8.RuneCountInString(text)
	if !ok || length > 10_000_000 || doc["text_sha256"] != digest([]byte(text)) {
		return nil, nil, invalid
	}
	rawTypes, ok := doc["types"].([]any)
	if !ok {
		return nil, nil, invalid
	}
	schemas := map[string]map[string]any{}
	for _, raw := range rawTypes {
		schema, err := object(raw, "name", "fields")
		if err != nil {
			return nil, nil, err
		}
		n, err := name(schema["name"])
		if err != nil {
			return nil, nil, err
		}
		if _, exists := schemas[n]; exists {
			return nil, nil, invalid
		}
		fields, err := object(schema["fields"])
		if err != nil {
			return nil, nil, err
		}
		for key, raw := range fields {
			if _, err := name(key); err != nil {
				return nil, nil, err
			}
			f, err := object(raw, "kind", "required", "nullable", "target_type")
			if err != nil {
				return nil, nil, err
			}
			kind, ok := f["kind"].(string)
			if !ok || !kinds[kind] {
				return nil, nil, invalid
			}
			if _, ok := f["required"].(bool); !ok {
				return nil, nil, invalid
			}
			if _, ok := f["nullable"].(bool); !ok {
				return nil, nil, invalid
			}
			if f["target_type"] != nil {
				if _, err := name(f["target_type"]); err != nil || (kind != "reference" && kind != "references") {
					return nil, nil, invalid
				}
			}
		}
		schemas[n] = schema
	}
	for _, schema := range schemas {
		for _, raw := range schema["fields"].(map[string]any) {
			f := raw.(map[string]any)
			if f["target_type"] != nil {
				if _, ok := schemas[f["target_type"].(string)]; !ok {
					return nil, nil, invalid
				}
			}
		}
	}
	annotations, ok := doc["annotations"].([]any)
	if !ok || len(annotations) > 1_000_000 {
		return nil, nil, invalid
	}
	byID := map[string]map[string]any{}
	for _, raw := range annotations {
		a, err := object(raw, "id", "type", "start", "end", "features")
		if err != nil {
			return nil, nil, err
		}
		id, err := name(a["id"])
		if err != nil {
			return nil, nil, err
		}
		if _, exists := byID[id]; exists {
			return nil, nil, invalid
		}
		typeName, err := name(a["type"])
		if err != nil {
			return nil, nil, err
		}
		if _, exists := schemas[typeName]; !exists {
			return nil, nil, invalid
		}
		start, err := offset(a["start"], length)
		if err != nil {
			return nil, nil, err
		}
		end, err := offset(a["end"], length)
		if err != nil || end < start {
			return nil, nil, invalid
		}
		if _, err := object(a["features"]); err != nil {
			return nil, nil, err
		}
		byID[id] = a
	}
	for _, a := range byID {
		features := a["features"].(map[string]any)
		fields := schemas[a["type"].(string)]["fields"].(map[string]any)
		for key := range features {
			if _, exists := fields[key]; !exists {
				return nil, nil, invalid
			}
		}
		for key, raw := range fields {
			f := raw.(map[string]any)
			value, exists := features[key]
			if !exists {
				if f["required"] == true {
					return nil, nil, invalid
				}
				continue
			}
			if value == nil && f["nullable"] == true {
				continue
			}
			if !featureValid(value, f, byID) {
				return nil, nil, invalid
			}
		}
	}
	return doc, schemas, nil
}

func featureValid(value any, f map[string]any, byID map[string]map[string]any) bool {
	kind := f["kind"].(string)
	switch kind {
	case "string":
		_, ok := value.(string)
		return ok
	case "integer":
		n, ok := value.(json.Number)
		return ok && !strings.ContainsAny(string(n), ".eE")
	case "number":
		_, ok := value.(json.Number)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok && featureDepth(value, 0)
	case "array":
		_, ok := value.([]any)
		return ok && featureDepth(value, 0)
	case "reference", "references":
		refs := []any{value}
		if kind == "references" {
			var ok bool
			refs, ok = value.([]any)
			if !ok {
				return false
			}
		}
		for _, ref := range refs {
			id, ok := ref.(string)
			if !ok {
				return false
			}
			target, exists := byID[id]
			if !exists || (f["target_type"] != nil && target["type"] != f["target_type"]) {
				return false
			}
		}
		return true
	}
	return false
}

func featureDepth(value any, depth int) bool {
	if depth > 31 {
		return false
	} // features root already consumes one level.
	switch v := value.(type) {
	case map[string]any:
		for _, x := range v {
			if !featureDepth(x, depth+1) {
				return false
			}
		}
	case []any:
		for _, x := range v {
			if !featureDepth(x, depth+1) {
				return false
			}
		}
	}
	return true
}

// Explicit Unicode White_Space set: no locale/tokenizer resource or normalization.
func space(r rune) bool {
	return r >= 9 && r <= 13 || r == 32 || r == 0x85 || r == 0xA0 || r == 0x1680 || r >= 0x2000 && r <= 0x200A || r == 0x2028 || r == 0x2029 || r == 0x202F || r == 0x205F || r == 0x3000
}

func processRequest(value any, desc map[string]any) (map[string]any, error) {
	r, err := object(value, "format", "operation_id", "step_id", "processor", "input_digest", "document")
	if err != nil || r["format"] != "corpusledger.processor-request.v1" || !reflect.DeepEqual(r["processor"], desc) {
		return nil, invalid
	}
	for _, key := range []string{"operation_id", "step_id", "input_digest"} {
		s, ok := r[key].(string)
		if !ok || (key == "input_digest" && !hashPattern.MatchString(s)) || (key != "input_digest" && !idPattern.MatchString(s)) {
			return nil, invalid
		}
	}
	doc, schemas, err := document(r["document"])
	if err != nil {
		return nil, err
	}
	if _, exists := schemas["token"]; exists {
		return nil, invalid
	}
	used := map[string]bool{}
	for _, raw := range doc["annotations"].([]any) {
		used[raw.(map[string]any)["id"].(string)] = true
	}
	runes := []rune(doc["text"].(string))
	output := []any{}
	outputBytes := 8192 // Reserve more than the bounded descriptor/identity envelope.
	for i := 0; i < len(runes); {
		if space(runes[i]) {
			i++
			continue
		}
		start := i
		for i < len(runes) && !space(runes[i]) {
			i++
		}
		id := fmt.Sprintf("demo.go.token.%d", len(output))
		if used[id] {
			return nil, invalid
		}
		if len(output)+len(used) >= 1_000_000 {
			return nil, invalid
		}
		annotation := map[string]any{"id": id, "type": "token", "start": start, "end": i, "features": map[string]any{"text": string(runes[start:i]), "position": len(output)}}
		encoded, err := json.Marshal(annotation)
		if err != nil || outputBytes+len(encoded)+1 > maxWire {
			return nil, invalid
		}
		outputBytes += len(encoded) + 1
		output = append(output, annotation)
	}
	return map[string]any{"format": "corpusledger.processor-result.v1", "operation_id": r["operation_id"], "step_id": r["step_id"], "processor": desc, "input_digest": r["input_digest"], "annotations": output, "duration_ms": 0}, nil
}
