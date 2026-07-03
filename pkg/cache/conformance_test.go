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

package cache_test

// The blank imports of pkg/cache/dacs and pkg/cache/noop below self-register each
// provider's conformance Expectations via init(). Adding a new provider package to the
// import list is all that is required for the registry-driven suite to discover it.
import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kaitov1beta1 "github.com/kaito-project/kaito/api/v1beta1"
	"github.com/kaito-project/kaito/pkg/cache"
	_ "github.com/kaito-project/kaito/pkg/cache/dacs"
	_ "github.com/kaito-project/kaito/pkg/cache/noop"
)

// TestProviderConformance_AutoDiscovered iterates every registered provider's
// Expectations and verifies the provider satisfies the shared PodMutations contract
// plus its own declared, provider-specific expectations. Because it is driven by the
// registry, any newly added provider is automatically included — and a provider that
// registers without a usable conformance factory fails the suite closed.
func TestProviderConformance_AutoDiscovered(t *testing.T) {
	exps := cache.ListExpectations()
	if len(exps) == 0 {
		t.Fatal("no cache provider expectations registered; expected at least noop and dacs")
	}
	t.Logf("discovered %d cache provider(s) for conformance", len(exps))

	ws := &kaitov1beta1.Workspace{
		ObjectMeta: metav1.ObjectMeta{Name: "conformance-ws", Namespace: "default"},
	}

	for _, e := range exps {
		e := e
		t.Run(string(e.Provider), func(t *testing.T) {
			// Fail closed: a discovered provider must supply a conformance factory.
			if e.NewForConformance == nil {
				t.Fatalf("provider %q registered Expectations without NewForConformance", e.Provider)
			}

			p := e.NewForConformance()
			if p == nil {
				t.Fatalf("provider %q NewForConformance returned nil", e.Provider)
			}
			if got := kaitov1beta1.CacheProvider(p.Name()); got != e.Provider {
				t.Fatalf("provider name mismatch: instance %q, expectations %q", got, e.Provider)
			}

			concerns := []cache.CacheConcern{
				cache.CacheConcernModelWeights,
				cache.CacheConcernKVCache,
			}
			for _, concern := range concerns {
				exp := e.ForConcern(concern)
				if !exp.Supported {
					continue
				}
				t.Run(string(concern), func(t *testing.T) {
					m, err := p.PodMutations(context.Background(), concern, ws, "test-model", "main", "")
					if err != nil {
						t.Fatalf("PodMutations(%s) returned error: %v", concern, err)
					}
					if m == nil {
						t.Fatalf("PodMutations(%s) returned nil mutations", concern)
					}
					for _, assertErr := range cache.AssertMutations(m, exp) {
						t.Errorf("[%s/%s] %v", e.Provider, concern, assertErr)
					}
				})
			}
		})
	}
}

// TestProviderConformance_ShippedProviders asserts that every provider shipped and
// wired into cmd/workspace/main.go declares conformance Expectations and produces a
// runnable instance whose name matches. This guards against adding a real provider
// without also registering it with the conformance suite.
//
// It intentionally checks the expectations registry rather than cache.List(), because
// the global provider registry is shared across the test binary and is deliberately
// polluted with fake providers by other unit tests in this package.
func TestProviderConformance_ShippedProviders(t *testing.T) {
	shipped := []kaitov1beta1.CacheProvider{"noop", "dacs"}
	for _, name := range shipped {
		e, ok := cache.GetExpectations(name)
		if !ok {
			t.Errorf("shipped provider %q declares no conformance Expectations", name)
			continue
		}
		if e.NewForConformance == nil {
			t.Errorf("shipped provider %q has no NewForConformance factory", name)
			continue
		}
		if got := kaitov1beta1.CacheProvider(e.NewForConformance().Name()); got != name {
			t.Errorf("provider %q factory produced instance named %q", name, got)
		}
	}
}
