// The (new) redis manager is intended to look like the officially supported memory manager.
package redis

import (
	"encoding/json"

	"github.com/go-redis/redis"
	. "github.com/ory/ladon"
	"github.com/pkg/errors"
)

// RedisManager is a redis implementation of Manager to store policies persistently.
type RedisManager struct {
	db        *redis.Client
	keyPrefix string
}

// NewRedisManager initializes a new RedisManager with no policies
func NewRedisManager(db *redis.Client, keyPrefix string) *RedisManager {
	return &RedisManager{
		db:        db,
		keyPrefix: keyPrefix,
	}
}

var (
	ErrPolicyExists  = errors.New("Policy exists")
	ErrBadConversion = errors.New("Could not convert policy from redis")
)

func (m *RedisManager) getKey(key string) string {
	return m.keyPrefix + key
}

// Create a new policy to RedisManager
func (m *RedisManager) Create(policy Policy) error {
	key := m.getKey(policy.GetID())
	if err := m.db.Get(key).Err(); err == nil {
		return ErrPolicyExists
	}

	p, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	cmd := m.db.Set(key, p, 0)

	if err := cmd.Err(); err != nil {
		return err
	}
	return nil
}

// GetAll retrieves all policies. (Equivelant of db.keys + db.Mget)
func (m *RedisManager) GetAll(limit int64, offset int64) (Policies, error) {
	keyscmd := m.db.Keys(m.getKey("*"))
	if err := keyscmd.Err(); err != nil {
		return nil, err
	}

	keys, err := keyscmd.Result()
	if err != nil {
		return nil, err
	}

	mgetcmd := m.db.MGet(keys...)
	if err := mgetcmd.Err(); err != nil {
		return nil, err
	}

	values := mgetcmd.Val()

	policies := make(Policies, len(values))
	for i, v := range values {
		p := &DefaultPolicy{}
		b := []byte(v.(string))
		// if !ok {
		// 	return nil, errors.Wrapf(ErrBadConversion, "value %+v is not a byte array", v)
		// }
		if err := json.Unmarshal(b, p); err != nil {
			return nil, errors.Wrap(ErrBadConversion, err.Error())
		}
		policies[i] = p
	}

	if offset+limit > int64(len(policies)) {
		limit = int64(len(policies))
		offset = 0
	}

	return policies[offset:limit], nil
}

// Get retrieves a policy.
func (m *RedisManager) Get(id string) (Policy, error) {
	var (
		key    = m.getKey(id)
		cmd    = m.db.Get(key)
		policy = &DefaultPolicy{}
	)

	if err := cmd.Err(); err != nil {
		return nil, ErrNotFound
	}
	b, err := cmd.Bytes()
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(b, policy); err != nil {
		return nil, errors.Wrap(ErrBadConversion, err.Error())
	}
	return policy, nil
}

// Delete removes a policy.
func (m *RedisManager) Delete(id string) error {
	key := m.getKey(id)
	if err := m.db.Get(key).Err(); err != nil {
		return ErrNotFound
	}
	if err := m.db.Del(key).Err(); err != nil {
		return err
	}

	return nil
}

// FindRequestCandidates returns candidates that could match the request object. It either returns
// a set that exactly matches the request, or a superset of it. If an error occurs, it returns nil and
// the error.
func (m *RedisManager) FindRequestCandidates(r *Request) (Policies, error) {
	keyscmd := m.db.Keys(m.getKey("*"))
	if err := keyscmd.Err(); err != nil {
		return nil, err
	}

	keys, err := keyscmd.Result()
	if err != nil {
		return nil, err
	}

	mgetcmd := m.db.MGet(keys...)
	if err := mgetcmd.Err(); err != nil {
		return nil, err
	}

	values := mgetcmd.Val()

	policies := make(Policies, len(values))
	for i, v := range values {
		p := &DefaultPolicy{}
		b := []byte(v.(string))
		// if !ok {
		// 	return nil, errors.Wrapf(ErrBadConversion, "value %+v is not a byte array", v)
		// }
		if err := json.Unmarshal(b, p); err != nil {
			return nil, errors.Wrap(ErrBadConversion, err.Error())
		}
		policies[i] = p
	}

	return policies, nil
}

func (m *RedisManager) Update(policy Policy) error {
	key := m.getKey(policy.GetID())
	if err := m.db.Get(key).Err(); err != nil {
		return ErrNotFound
	}
	b, err := json.Marshal(policy)
	if err != nil {
		return errors.Wrap(ErrBadConversion, err.Error())
	}
	if err := m.db.Set(key, b, 0).Err(); err != nil {
		return err
	}

	return nil
}
