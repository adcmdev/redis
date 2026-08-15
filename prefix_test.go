package redis

import (
	"testing"
	"time"
)

// Two clients, two prefixes, one shared pool. Fails if the old singleton
// behavior (first caller's prefix wins globally) ever comes back.
func TestPrefixIsolation(t *testing.T) {
	a, err := NewClient(CreateNewRedisDTO{Prefix: "roles:"})
	if err != nil {
		t.Skipf("no redis at localhost:6379: %v", err)
	}
	b, _ := NewClient(CreateNewRedisDTO{Prefix: "users:"})

	if a.(*client).redisClient != b.(*client).redisClient {
		t.Fatal("clients must share one pool")
	}

	if err := a.Set("k", []byte("A"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := b.Set("k", []byte("B"), time.Minute); err != nil {
		t.Fatal(err)
	}
	defer a.Delete("k")
	defer b.Delete("k")

	got, err := a.Get("k")
	if err != nil || string(got) != "A" {
		t.Fatalf("prefix a leaked: got %q err %v", got, err)
	}
	got, err = b.Get("k")
	if err != nil || string(got) != "B" {
		t.Fatalf("prefix b leaked: got %q err %v", got, err)
	}

	keys, err := a.GetAllKeys("")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if len(k) < 6 || k[:6] != "roles:" {
			t.Fatalf("GetAllKeys crossed prefixes: %q", k)
		}
	}
}
