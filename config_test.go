// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlebigquery

import (
	"testing"
)

func TestParseNoConfig(t *testing.T) {
	cfg := map[string]string{}
	_, err := ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got no error")
	}
}

func TestParseSourceConfigAllConfigPresent(t *testing.T) {
	cfg := map[string]string{}
	cfg[ConfigServiceAccount] = "test"
	cfg[ConfigProjectID] = "test"
	cfg[ConfigDatasetID] = "test"
	cfg[ConfigLocation] = "test"
	cfg[ConfigTableID] = "testTable"
	cfg[ConfigPrimaryKeyColName] = "primaryKey"

	_, err := ParseSourceConfig(cfg)
	if err != nil {
		t.Errorf("parse source config, got error %v", err)
	}
}

func TestParseSourceConfigPartialConfig(t *testing.T) {
	cfg := map[string]string{}
	delete(cfg, ConfigServiceAccount)
	cfg[ConfigProjectID] = "test"
	cfg[ConfigDatasetID] = "test"
	cfg[ConfigLocation] = "test"

	_, err := ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}

	cfg[ConfigServiceAccount] = "test"
	delete(cfg, ConfigProjectID)
	cfg[ConfigDatasetID] = "test"

	_, err = ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}

	cfg[ConfigServiceAccount] = "test"
	cfg[ConfigProjectID] = "test"
	delete(cfg, ConfigDatasetID)

	_, err = ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}

	cfg[ConfigDatasetID] = "test"
	delete(cfg, ConfigLocation)

	_, err = ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}
}

func TestSpecification(t *testing.T) {
	Specification()
}
