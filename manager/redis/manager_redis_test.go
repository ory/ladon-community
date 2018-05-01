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

func contains(s []ladon.Policy, p ladon.Policy) bool {
	for _, v := range s {
		if cmp.Equal(v, p) {
			return true
		}
	}
	return false
}

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

func TestUpdate(t *testing.T) {
	m := NewRedisManager(db, "update")

	t.Run("Successfully update a policy", func(t *testing.T) {
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

		policy.Subjects = []string{"2", "3", "4"}
		if err := m.Update(policy); err != nil {
			t.Fatal(err)
		}

		u, err := m.Get(policy.GetID())
		if err != nil {
			t.Fatal(err)
		}

		if cmp.Equal(u, policy) != true {
			t.Fatalf("Unexpected policy from 'Get' after 'Update'\n%s", cmp.Diff(u, policy))
		}
	})

	t.Run("Attempt to update a policy that doesn't exist", func(t *testing.T) {
		if err := m.Update(&ladon.DefaultPolicy{
			ID: "this policy does not exist",
		}); err != ladon.ErrNotFound {
			t.Fatal("No error returned when attempting to update a policy that does not exist")
		}
	})
}

func TestDelete(t *testing.T) {
	m := NewRedisManager(db, "delete")

	t.Run("Successfully delete a policy", func(t *testing.T) {
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

		if err := m.Delete(policy.GetID()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Attempt to delete a policy that does not exist", func(t *testing.T) {
		if err := m.Delete("this policy does not exist"); err != ladon.ErrNotFound {
			t.Fatal("No error returned when attempting to delete a policy that does not exist")
		}
	})
}

func TestFindRequestCandidate(t *testing.T) {
	m := NewRedisManager(db, "find")

	policies := ladon.Policies{
		&ladon.DefaultPolicy{
			ID:         "test-policy-1",
			Conditions: ladon.Conditions{},
		},
		&ladon.DefaultPolicy{
			ID:         "test-policy-2",
			Conditions: ladon.Conditions{},
		},
		&ladon.DefaultPolicy{
			ID:         "test-policy-3",
			Conditions: ladon.Conditions{},
		},
	}

	for _, v := range policies {
		if err := m.Create(v); err != nil {
			t.Fatal(err)
		}
	}

	p, err := m.FindRequestCandidates(nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range policies {
		if contains(p, v) != true {
			t.Fatalf("Policy %+v not found", v)
		}
	}
}
