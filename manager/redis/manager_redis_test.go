package redis

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/go-redis/redis"
	"github.com/google/go-cmp/cmp"
	"github.com/ory/ladon"
	"gopkg.in/ory-am/dockertest.v3"
)

var db *redis.Client

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("redis", "3.2.11", nil)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	settings, err := redis.ParseURL(fmt.Sprintf("redis://localhost:%s", resource.GetPort("6379/tcp")))
	if err != nil {
		log.Fatal(err.Error())
	}
	db = redis.NewClient(settings)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		_, err := db.Ping().Result()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestCreate(t *testing.T) {
	m := NewRedisManager(db, "create")

	t.Run("Successfully create a single resource", func(t *testing.T) {
		policy := &ladon.DefaultPolicy{
			ID: "example-policy-1",
		}
		err := m.Create(policy)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Error when creating duplicate policies", func(t *testing.T) {
		policy := &ladon.DefaultPolicy{
			ID: "example-policy-2",
		}
		err := m.Create(policy)
		if err != nil {
			t.Fatal(err)
		}
		err = m.Create(policy)
		if err != ErrPolicyExists {
			t.Fatal("No error returned when creating duplicate policies")
		}
	})
	// Create the same resource twice should return an error
}

func TestGet(t *testing.T) {
	m := NewRedisManager(db, "get")

	t.Run("Successfully retrieve a single policy", func(t *testing.T) {
		// Some weirdness with json.Marshal requires us to initialize Conditions for this test
		policy := &ladon.DefaultPolicy{
			ID:         "example-policy-1",
			Subjects:   []string{"1", "2"},
			Conditions: ladon.Conditions{},
		}
		err := m.Create(policy)
		if err != nil {
			t.Fatal(err)
		}
		p, err := m.Get(policy.GetID())
		if err != nil {
			t.Fatal(err)
		}

		if cmp.Equal(policy, p) != true {
			t.Fatalf("Unexpected policy.\n%s", cmp.Diff(policy, p))
		}
	})

	t.Run("Attempt to retrieve a non-existent policy", func(t *testing.T) {
		_, err := m.Get("policy that doesn't exist")
		if err != ladon.ErrNotFound {
			t.Fatal("Attempting to get a policy that doesn't exist should return an error")
		}
	})
}
