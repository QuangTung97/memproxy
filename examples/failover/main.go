package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/QuangTung97/memproxy"
	mcitem "github.com/QuangTung97/memproxy/item"
	"github.com/QuangTung97/memproxy/proxy"
	"time"
)

// User ...
type User struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

// Marshal ...
func (u User) Marshal() ([]byte, error) {
	return json.Marshal(u)
}

func unmarshalUser(data []byte) (User, error) {
	var u User
	err := json.Unmarshal(data, &u)
	return u, err
}

// UserKey ...
type UserKey struct {
	ID int64
}

// String ...
func (u UserKey) String() string {
	return fmt.Sprintf("users:%d", u.ID)
}

func main() {
	mc, closeFun, err := proxy.NewSimpleReplicatedMemcache([]proxy.SimpleServerConfig{
		{
			ID:   1,
			Host: "localhost",
			Port: 11211,
		},
		{
			ID:   2,
			Host: "localhost",
			Port: 11212,
		},
	}, 3)
	if err != nil {
		panic(err)
	}
	defer closeFun()

	sessProvider := memproxy.NewSessionProvider()
	userSeq := 0

	for {
		doGetFromCache(mc, sessProvider, &userSeq)
		time.Sleep(1 * time.Second)
	}
}

func doGetFromCache(
	mc memproxy.Memcache, sessProvider memproxy.SessionProvider,
	userSeq *int,
) {
	pipe := mc.Pipeline(context.Background(), sessProvider.New())
	defer pipe.Finish()

	*userSeq++
	id := *userSeq % 11

	userItem := mcitem.New[User, UserKey](
		pipe, unmarshalUser,
		func(ctx context.Context, key UserKey) func() (User, error) {
			fmt.Println("DO Fill with Key:", key)
			return func() (User, error) {
				return User{
					ID:       int64(id),
					Username: fmt.Sprintf("username:%d", *userSeq),
				}, nil
			}
		},
	)

	fn := userItem.Get(context.Background(), UserKey{
		ID: int64(id),
	})
	user, err := fn()
	fmt.Println(user, err)
}
