// Copyright (c) KAITO authors.
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

package tachyon

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModelBlobPath(t *testing.T) {
	tests := []struct {
		name         string
		blobEndpoint string
		container    string
		prefix       string
		modelID      string
		revision     string
		expected     string
	}{
		{
			name:         "standard HF model with revision",
			blobEndpoint: "https://myaccount.blob.core.windows.net",
			container:    "models",
			prefix:       "",
			modelID:      "microsoft/phi-4",
			revision:     "abc123",
			expected:     "https://myaccount.blob.core.windows.net/models/kaito-models/microsoft/phi-4/abc123",
		},
		{
			name:         "default revision when empty",
			blobEndpoint: "https://myaccount.blob.core.windows.net",
			container:    "models",
			prefix:       "",
			modelID:      "microsoft/phi-4",
			revision:     "",
			expected:     "https://myaccount.blob.core.windows.net/models/kaito-models/microsoft/phi-4/main",
		},
		{
			name:         "custom prefix",
			blobEndpoint: "https://myaccount.blob.core.windows.net",
			container:    "cache",
			prefix:       "custom-prefix",
			modelID:      "meta-llama/Llama-3.3-70B-Instruct",
			revision:     "main",
			expected:     "https://myaccount.blob.core.windows.net/cache/custom-prefix/meta-llama/Llama-3.3-70B-Instruct/main",
		},
		{
			name:         "trailing slash on endpoint",
			blobEndpoint: "https://myaccount.blob.core.windows.net/",
			container:    "models",
			prefix:       "",
			modelID:      "microsoft/phi-4",
			revision:     "main",
			expected:     "https://myaccount.blob.core.windows.net/models/kaito-models/microsoft/phi-4/main",
		},
		{
			name:         "single-segment model name",
			blobEndpoint: "https://myaccount.blob.core.windows.net",
			container:    "models",
			prefix:       "",
			modelID:      "phi-4",
			revision:     "main",
			expected:     "https://myaccount.blob.core.windows.net/models/kaito-models/phi-4/main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ModelBlobPath(tt.blobEndpoint, tt.container, tt.prefix, tt.modelID, tt.revision)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestModelBlobRelativePath(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		modelID  string
		revision string
		expected string
	}{
		{
			name:     "default prefix and revision",
			prefix:   "",
			modelID:  "microsoft/phi-4",
			revision: "",
			expected: "kaito-models/microsoft/phi-4/main",
		},
		{
			name:     "custom prefix with revision",
			prefix:   "my-models",
			modelID:  "meta-llama/Llama-3.3-70B-Instruct",
			revision: "abc123",
			expected: "my-models/meta-llama/Llama-3.3-70B-Instruct/abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ModelBlobRelativePath(tt.prefix, tt.modelID, tt.revision)
			assert.Equal(t, tt.expected, result)
		})
	}
}
