// Original, bounded attachment upload oracle, not a general CorpusLedger SDK.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"
	"unicode/utf8"
)

const rawLimit = 4 * 1024 * 1024
const wireLimit = 16 * 1024 * 1024

type config struct {
	Endpoint         string `json:"endpoint"`
	EventID          string `json:"event_id"`
	Name             string `json:"name"`
	PayloadFile      string `json:"payload_file"`
	ExpectedRevision int64  `json:"expected_revision"`
	ExpectedDigest   string `json:"expected_digest"`
	CommandID        string `json:"command_id"`
}

func strictJSON(raw []byte) (map[string]any, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("invalid JSON")
	}
	// encoding/json replaces unpaired surrogate escapes; the wire contract must
	// reject them instead of silently changing an identity-bearing string.
	for index := 0; index < len(raw); index++ {
		if raw[index] != '\\' {
			continue
		}
		index++
		if index >= len(raw) || raw[index] != 'u' {
			continue
		}
		if index+4 >= len(raw) {
			return nil, errors.New("invalid Unicode escape")
		}
		point, err := strconv.ParseUint(string(raw[index+1:index+5]), 16, 16)
		if err != nil {
			return nil, err
		}
		index += 4
		if point >= 0xd800 && point <= 0xdbff {
			if index+6 >= len(raw) || raw[index+1] != '\\' || raw[index+2] != 'u' {
				return nil, errors.New("unpaired surrogate")
			}
			low, err := strconv.ParseUint(string(raw[index+3:index+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return nil, errors.New("unpaired surrogate")
			}
			index += 6
		} else if point >= 0xdc00 && point <= 0xdfff {
			return nil, errors.New("unpaired surrogate")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	nodes := 0
	var visit func(int) (any, error)
	visit = func(depth int) (any, error) {
		nodes++
		if depth > 40 || nodes > 2000000 {
			return nil, errors.New("JSON bounds")
		}
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if delimiter, ok := token.(json.Delim); ok {
			if delimiter == '{' {
				result := map[string]any{}
				for decoder.More() {
					keyToken, err := decoder.Token()
					key, ok := keyToken.(string)
					if err != nil || !ok {
						return nil, errors.New("invalid object")
					}
					if _, exists := result[key]; exists {
						return nil, errors.New("duplicate JSON key")
					}
					result[key], err = visit(depth + 1)
					if err != nil {
						return nil, err
					}
				}
				_, err = decoder.Token()
				return result, err
			}
			if delimiter == '[' {
				result := []any{}
				for decoder.More() {
					child, err := visit(depth + 1)
					if err != nil {
						return nil, err
					}
					result = append(result, child)
				}
				_, err = decoder.Token()
				return result, err
			}
			return nil, errors.New("invalid delimiter")
		}
		return token, nil
	}
	value, err := visit(0)
	if err != nil {
		return nil, err
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON")
	}
	result, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("object required")
	}
	return result, nil
}

func integer(value any) (int64, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, errors.New("integer required")
	}
	return strconv.ParseInt(string(number), 10, 64)
}

func run() error {
	rawConfig, err := io.ReadAll(io.LimitReader(os.Stdin, 65537))
	if err != nil || len(rawConfig) > 65536 {
		return errors.New("configuration bound")
	}
	if _, err = strictJSON(rawConfig); err != nil {
		return err
	}
	var settings config
	decoder := json.NewDecoder(bytes.NewReader(rawConfig))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&settings); err != nil {
		return err
	}
	endpoint, err := url.Parse(settings.Endpoint)
	shaPattern := regexp.MustCompile(`^[0-9a-f]{64}$`)
	if err != nil || endpoint.Scheme != "http" || endpoint.Hostname() != "127.0.0.1" ||
		endpoint.Port() == "" || endpoint.User != nil || endpoint.Path != "" ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" || settings.ExpectedRevision < 1 ||
		!shaPattern.MatchString(settings.ExpectedDigest) {
		return errors.New("invalid loopback configuration")
	}
	port, err := strconv.Atoi(endpoint.Port())
	if err != nil || port < 1 || port > 65535 {
		return errors.New("invalid port")
	}
	token := os.Getenv("CORPUSLEDGER_ATTACHMENT_ORACLE_TOKEN")
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{32,128}$`).MatchString(token) {
		return errors.New("credential required")
	}
	file, err := os.Open(settings.PayloadFile)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > rawLimit {
		file.Close()
		return errors.New("payload file bound")
	}
	data, err := io.ReadAll(io.LimitReader(file, rawLimit+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil || len(data) > rawLimit {
		return errors.New("payload bound")
	}
	checksum := sha256.Sum256(data)
	sha := hex.EncodeToString(checksum[:])
	body, err := json.Marshal(map[string]any{
		"format": "corpusledger.event-command.v1", "command": "attachment_attach",
		"arguments": map[string]any{
			"event_id": settings.EventID, "name": settings.Name,
			"data": base64.StdEncoding.EncodeToString(data), "media_type": "application/octet-stream",
			"expected_revision": settings.ExpectedRevision, "expected_digest": settings.ExpectedDigest,
			"command_id": settings.CommandID,
		},
	})
	if err != nil || len(body) > wireLimit {
		return errors.New("wire bound")
	}
	transport := &http.Transport{Proxy: nil, DisableCompression: true,
		MaxResponseHeaderBytes: 65536, ResponseHeaderTimeout: 15 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, err := http.NewRequest(http.MethodPost, settings.Endpoint+"/v1/events", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.ContentLength > wireLimit || response.Header.Get("Content-Encoding") != "" {
		return errors.New("rejected HTTP response")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, wireLimit+1))
	if err != nil || len(raw) > wireLimit {
		return errors.New("response bound")
	}
	envelope, err := strictJSON(raw)
	if err != nil || len(envelope) != 4 || envelope["format"] != "corpusledger.event-response.v1" ||
		envelope["command"] != "attachment_attach" || envelope["ok"] != true {
		return errors.New("invalid response envelope")
	}
	result, ok := envelope["result"].(map[string]any)
	if !ok || result["event_id"] != settings.EventID || result["parent_digest"] != settings.ExpectedDigest {
		return errors.New("revision identity mismatch")
	}
	revision, err := integer(result["revision"])
	digest, ok := result["digest"].(string)
	if err != nil || revision != settings.ExpectedRevision+1 || !ok || !shaPattern.MatchString(digest) {
		return errors.New("revision pin mismatch")
	}
	event, ok := result["event"].(map[string]any)
	if !ok || event["format"] != "corpusledger.annotation-event.v2" || event["id"] != settings.EventID {
		return errors.New("event identity mismatch")
	}
	attachments, ok := event["attachments"].([]any)
	if !ok {
		return errors.New("missing attachment manifest")
	}
	matches := 0
	for _, value := range attachments {
		manifest, ok := value.(map[string]any)
		if !ok {
			return errors.New("invalid manifest")
		}
		if manifest["name"] == settings.Name {
			matches++
			size, err := integer(manifest["size"])
			if err != nil || len(manifest) != 4 || size != int64(len(data)) || manifest["sha256"] != sha ||
				manifest["media_type"] != "application/octet-stream" {
				return errors.New("binary manifest mismatch")
			}
		}
	}
	if matches != 1 {
		return errors.New("attachment identity mismatch")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"format": "corpusledger.attachment-upload-oracle.v1", "verified": true,
		"event_id": settings.EventID, "revision": revision, "digest": digest,
		"sha256": sha, "size": len(data), "name": settings.Name,
	})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "attachment_upload_failed")
		os.Exit(2)
	}
}
