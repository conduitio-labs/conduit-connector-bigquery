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

// func TestAcceptance(t *testing.T) {
// 	cfg := testConfigMap()

// 	sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
// 		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
// 			Connector: sdk.Connector{ // Note that this variable should rather be created globally in `connector.go`
// 				NewSpecification: Specification,
// 				NewSource:        NewSource,
// 				NewDestination:   NewDestination,
// 			},
// 			SourceConfig:      cfg,
// 			DestinationConfig: cfg,

// 			BeforeTest: func(t *testing.T) {
// 				// use new topic for each test
// 				cfg[Topic] = "test-topic-" + uuid.NewString()
// 			},
// 		},
// 	})
// }
