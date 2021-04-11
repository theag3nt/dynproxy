package dynproxy

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

type Storage interface {
	Load(key string) (endpoint *url.URL, ok bool)
	Store(key string, endpoint *url.URL)
	Delete(key string)
	Values() (endpoints map[string]*url.URL)
}

type MemoryStore struct {
	mu sync.RWMutex
	m  map[string]*url.URL
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		m: make(map[string]*url.URL),
	}
}

func (ms *MemoryStore) Load(key string) (endpoint *url.URL, ok bool) {
	ms.mu.RLock()
	endpoint, ok = ms.m[key]
	ms.mu.RUnlock()
	return
}

func (ms *MemoryStore) Store(key string, endpoint *url.URL) {
	ms.mu.Lock()
	ms.m[key] = endpoint
	ms.mu.Unlock()
}

func (ms *MemoryStore) Delete(key string) {
	ms.mu.Lock()
	delete(ms.m, key)
	ms.mu.Unlock()
}

func (ms *MemoryStore) Values() map[string]*url.URL {
	copy := make(map[string]*url.URL)
	ms.mu.RLock()
	for k, v := range ms.m {
		copy[k] = v
	}
	ms.mu.RUnlock()
	return copy
}

var _ Storage = &MemoryStore{}

type SqliteStore struct {
	db *sql.DB
}

func NewSqliteStore(db *sql.DB) *SqliteStore {
	return &SqliteStore{db}
}

func (ss *SqliteStore) Init() (err error) {
	_, err = ss.db.Exec(`
		CREATE TABLE IF NOT EXISTS dynproxy (
			key TEXT PRIMARY KEY ON CONFLICT REPLACE,
			endpoint TEXT NOT NULL
		);
	`)
	return

}

func (ss *SqliteStore) Load(key string) (*url.URL, bool) {
	row := ss.db.QueryRow(`SELECT endpoint FROM dynproxy WHERE key = ?;`, key)
	var value string
	if err := row.Scan(&value); err != nil {
		return nil, false
	}
	endpoint, err := url.Parse(value)
	if err != nil {
		return nil, false
	}
	return endpoint, true
}

func (ss *SqliteStore) Store(key string, endpoint *url.URL) {
	ss.db.Exec(`INSERT INTO dynproxy (key, endpoint) VALUES (?, ?);`, key, endpoint.String())
}

func (ss *SqliteStore) Delete(key string) {
	ss.db.Exec(`DELETE FROM dynproxy WHERE key = ?;`, key)
}

func (ss *SqliteStore) Values() map[string]*url.URL {
	values := make(map[string]*url.URL)
	rows, err := ss.db.Query(`SELECT key, endpoint FROM dynproxy;`)
	if err != nil {
		return values
	}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			continue
		}
		endpoint, err := url.Parse(value)
		if err != nil {
			continue
		}
		values[key] = endpoint
	}
	return values
}

var _ Storage = &SqliteStore{}

type ReadOnlyStore struct {
	Storage
}

func NewReadOnlyStore(storage Storage) *ReadOnlyStore {
	return &ReadOnlyStore{storage}
}

func (ros *ReadOnlyStore) Delete(key string) {}

var _ Storage = &ReadOnlyStore{}

type HttpStore struct {
	Storage

	username string
	password string
}

func NewHttpStore(storage Storage) *HttpStore {
	return &HttpStore{Storage: storage}
}

func NewHttpStoreWithAuth(storage Storage, username, password string) *HttpStore {
	return &HttpStore{
		Storage:  storage,
		username: username,
		password: password,
	}
}

func (hs *HttpStore) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if !hs.authenticate(rw, req) {
		return
	}

	req.ParseForm()
	form := req.Form

	switch req.Method {
	case "GET":
		hs.handleGet(rw, form)
	case "PUT":
		hs.handlePut(rw, form)
	case "DELETE":
		hs.handleDelete(rw, form)
	default:
		rw.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (hs *HttpStore) authenticate(rw http.ResponseWriter, req *http.Request) bool {
	if hs.username == "" || hs.password == "" {
		return true
	}
	if user, pass, ok := req.BasicAuth(); !ok || user != hs.username || pass != hs.password {
		rw.Header().Set("WWW-Authenticate", "Basic realm=\"dynproxy\"")
		rw.WriteHeader(http.StatusUnauthorized)
		return false
	}
	return true
}

func (hs *HttpStore) handleGet(rw http.ResponseWriter, form url.Values) {
	values := url.Values{}
	for key := range form {
		if endpoint, ok := hs.Load(key); ok {
			values.Set(key, endpoint.String())
		}
	}
	if len(values) == 0 {
		if len(form) > 0 {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		for key, endpoint := range hs.Values() {
			values.Set(key, endpoint.String())
		}
	}
	rw.Header().Set("Content-Type", "application/x-www-form-urlencoded")
	rw.Write([]byte(values.Encode()))
}

func (hs *HttpStore) handlePut(rw http.ResponseWriter, form url.Values) {
	for key := range form {
		value := strings.TrimSpace(form.Get(key))
		if value == "" {
			continue
		}
		endpoint, err := url.Parse(value)
		if err != nil {
			// TODO: Include err into the error message
			http.Error(rw, fmt.Sprintf("invalid url for: %s", key), http.StatusBadRequest)
			return
		}
		if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
			http.Error(rw, fmt.Sprintf("invalid or missing scheme for: %s", key), http.StatusBadRequest)
			return
		}
		hs.Store(key, endpoint)
	}
	rw.WriteHeader(http.StatusOK)
}

func (hs *HttpStore) handleDelete(rw http.ResponseWriter, form url.Values) {
	for key := range form {
		hs.Delete(key)
	}
	rw.WriteHeader(http.StatusOK)
}

var _ Storage = &HttpStore{}
var _ http.Handler = &HttpStore{}
