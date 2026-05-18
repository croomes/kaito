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
	"fmt"
	"net/url"
	"strings"
)

const (
	// DefaultBlobPrefix is the prefix used in blob storage for cached model weights.
	DefaultBlobPrefix = "kaito-models"

	// DefaultRevision is used when no revision is specified.
	DefaultRevision = "main"
)

// ModelBlobPath derives the deterministic blob path for a model.
// The convention is:
//
//	<blobEndpoint>/<container>/<prefix>/<org>/<model>/<revision>/
//
// Both the prewarm Job (upload target) and PodMutations (runtime read source)
// use this function to ensure consistency.
func ModelBlobPath(blobEndpoint, container, prefix, modelID, revision string) string {
	if prefix == "" {
		prefix = DefaultBlobPrefix
	}
	if revision == "" {
		revision = DefaultRevision
	}

	// URL-escape path segments to handle any special characters.
	parts := strings.SplitN(modelID, "/", 2)
	var encodedModel string
	if len(parts) == 2 {
		encodedModel = url.PathEscape(parts[0]) + "/" + url.PathEscape(parts[1])
	} else {
		encodedModel = url.PathEscape(modelID)
	}

	return fmt.Sprintf("%s/%s/%s/%s/%s",
		strings.TrimRight(blobEndpoint, "/"),
		url.PathEscape(container),
		prefix,
		encodedModel,
		url.PathEscape(revision),
	)
}

// ModelBlobRelativePath returns just the relative path within the container,
// without the endpoint prefix. Used for prewarm uploads where the endpoint
// is configured separately.
func ModelBlobRelativePath(prefix, modelID, revision string) string {
	if prefix == "" {
		prefix = DefaultBlobPrefix
	}
	if revision == "" {
		revision = DefaultRevision
	}

	parts := strings.SplitN(modelID, "/", 2)
	var encodedModel string
	if len(parts) == 2 {
		encodedModel = url.PathEscape(parts[0]) + "/" + url.PathEscape(parts[1])
	} else {
		encodedModel = url.PathEscape(modelID)
	}

	return fmt.Sprintf("%s/%s/%s", prefix, encodedModel, url.PathEscape(revision))
}

const (
	// DefaultStoragePath is the base path where StorageIntercept intercepts reads.
	// vLLM's --model path will be a subdirectory of this.
	DefaultStoragePath = "/mnt/models"
)

// ModelLocalPath returns the local filesystem path for a model when using
// StorageIntercept. vLLM's --model flag should point to this path.
// StorageIntercept maps reads from <storagePath>/<relativeBlobPath>/file
// to <blobContainer>/<relativeBlobPath>/file in blob storage.
func ModelLocalPath(storagePath, prefix, modelID, revision string) string {
	if storagePath == "" {
		storagePath = DefaultStoragePath
	}
	relativePath := ModelBlobRelativePath(prefix, modelID, revision)
	return fmt.Sprintf("%s/%s", strings.TrimRight(storagePath, "/"), relativePath)
}
