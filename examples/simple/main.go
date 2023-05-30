package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

type service struct {
	db       *sqlx.DB
	memcache memproxy.Memcache
}

var dropTableSQL = `
DROP TABLE IF EXISTS customer
`

var createTableSQL = `
CREATE TABLE customer (
    id INT PRIMARY KEY,
    username VARCHAR(100) NOT NULL
)
`

// Customer ...
type Customer struct {
	ID       int64  `db:"id" json:"id"`
	Username string `db:"username" json:"username"`
}

// Marshal ...
func (c Customer) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// GetKey ...
func (c Customer) GetKey() CustomerKey {
	return CustomerKey{ID: c.ID}
}

func unmarshalCustomer(data []byte) (Customer, error) {
	var c Customer
	err := json.Unmarshal(data, &c)
	return c, err
}

// CustomerKey ...
type CustomerKey struct {
	ID int64
}

func (k CustomerKey) String() string {
	return fmt.Sprintf("customers:%d", k.ID)
}

func (s *service) getCustomersFromDB(ctx context.Context, keys []CustomerKey) ([]Customer, error) {
	ids := make([]int64, 0, len(keys))
	for _, k := range keys {
		ids = append(ids, k.ID)
	}

	fmt.Println("Multi Get from Database with IDs =", ids)

	query, args, err := sqlx.In(`SELECT id, username FROM customer WHERE id IN (?)`, ids)
	if err != nil {
		return nil, err
	}

	var customers []Customer
	err = s.db.SelectContext(ctx, &customers, query, args...)
	return customers, err
}

func (s *service) newCustomerItem(pipe memproxy.Pipeline) *item.Item[Customer, CustomerKey] {
	return item.New[Customer, CustomerKey](
		pipe,
		unmarshalCustomer,
		item.NewMultiGetFiller[Customer, CustomerKey](
			s.getCustomersFromDB,
			Customer.GetKey,
		),
	)
}

func main() {
	client, err := memcache.New("localhost:11211", 3)
	if err != nil {
		panic(err)
	}
	mc := memproxy.NewPlainMemcache(client)

	db := sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/memtest?")

	db.MustExec(dropTableSQL)
	db.MustExec(createTableSQL)

	db.MustExec(`
INSERT INTO customer (id, username)
VALUES (11, "user01"), (12, "user02")
`)

	svc := &service{
		db:       db,
		memcache: mc,
	}

	pipe := mc.Pipeline(context.Background())
	customerItem := svc.newCustomerItem(pipe)

	fn1 := customerItem.Get(context.Background(), CustomerKey{ID: 11})
	fn2 := customerItem.Get(context.Background(), CustomerKey{ID: 12})

	// not found
	fn3 := customerItem.Get(context.Background(), CustomerKey{ID: 13})

	c1, err := fn1()
	fmt.Println("CUSTOMER 01:", c1, err)

	c2, err := fn2()
	fmt.Println("CUSTOMER 02:", c2, err)

	c3, err := fn3()
	fmt.Println("CUSTOMER 03:", c3, err)

	// should use defer pipe.Finish()
	pipe.Finish()

	// The 3 keys: customers:11, customers:12, customers:13 will exist in the memcached server
	// Can check using: telnet localhost 11211
	// get customers:11

	// =============================================
	// Do Get Again
	// =============================================
	pipe = mc.Pipeline(context.Background())
	customerItem = svc.newCustomerItem(pipe)

	fn1 = customerItem.Get(context.Background(), CustomerKey{ID: 11})
	fn2 = customerItem.Get(context.Background(), CustomerKey{ID: 12})

	c1, err = fn1()
	fmt.Println("CUSTOMER 01 AGAIN:", c1, err)
	c2, err = fn2()
	fmt.Println("CUSTOMER 02 AGAIN:", c2, err)

	pipe.Finish()
}
