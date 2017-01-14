// Package service provides the HTTP server for accessing the distributed key-value store.
// It also provides the endpoint for other nodes to join an existing cluster.
package service

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"github.com/laohanlinux/go-logger/logger"
	"sync"
)

// Store is the interface Raft-backed key-value stores must implement.
type Store interface {
	// Get returns the value for the given key.
	Get(key string) (string, error)

	// Set sets the value for the given key, via distributed consensus.
	Set(key, value string) error

	// Delete removes the given key, via distributed consensus.
	Delete(key string) error

	// Join joins the node, reachable at addr, to the cluster.
	Join(addr string) error
}

// Service provides HTTP  and  RESP service.
type Service struct {
	httpAddr     string
	httpListener net.Listener

	respAddr     string
	respListener net.Listener

	respLock      sync.Mutex //互斥锁
	respClients   map[*RespClient] struct{}

	store        Store
}

// New returns an uninitialized   service.  构造函数
func New(httpAddr string,respAddr string, store Store) *Service {
	clientmap := make(map[*RespClient]struct{})
	Init()
	return &Service{
		httpAddr:  httpAddr,
		respAddr: respAddr,
		store: store,
		respClients:clientmap,
	}
}


func (s * Service) addRespClient(client *RespClient)  {
	s.respLock.Lock()
	s.respClients[client]=struct {}{}
	s.respLock.Unlock()
}


func (s * Service) delRespClient(client *RespClient)  {
	s.respLock.Lock()
	delete(s.respClients,client)
	s.respLock.Unlock()
}

//start  http  and RESP service
func (s * Service) StartService() error{
	err:= s.HttpServiceStart()
	if err!= nil{
		return err
	}
	logger.Infof("start http service")

	go func(){
		err = s.RESPServiceStart()
		if err!= nil{
			logger.Warnf("start resp service  fail")
		}
	}()

	return  nil
}


// Start start the  RESP service.
func (s *Service) RESPServiceStart() error {

	ln, err := net.Listen("tcp", s.respAddr)
	if err != nil {
		logger.Warnf("lisent  respAddr fail,respAddr=%s,err=%s",s.respAddr,err)
		return err
	}

	s.respListener = ln
	logger.Infof("start resp service")

	for {
		c, err := s.respListener.Accept()
		if err != nil {
			logger.Warnf("accept error:%s", err)
			break
		}
		// start a new goroutine to handle
		// the new connection.
		NewRespClient(c,s)
	}

	return nil
}

// Start starts the http service.
func (s *Service) HttpServiceStart() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.httpAddr)
	if err != nil {
		logger.Warnf("lisent  http fail,httpAddr=%s,err=%s",s.respAddr,err)
		return err
	}
	s.httpListener = ln

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.httpListener)
		if err != nil {
			logger.Warn("HTTP serve: %s", err)
		}
	}()

	return nil
}

// Close closes the service.
func (s *Service) Close() {
	s.httpListener.Close()
	return
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/key") {
		s.handleKeyRequest(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Join(remoteAddr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	getKey := func() string {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 {
			return ""
		}
		return parts[2]
	}

	switch r.Method {
	case "GET":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
		}
		v, err := s.store.Get(k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		b, err := json.Marshal(map[string]string{k: v})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		io.WriteString(w, string(b))

	case "POST":
		// Read the value from the POST body.
		m := map[string]string{}
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for k, v := range m {
			if err := s.store.Set(k, v); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

	case "DELETE":
		k := getKey()
		if k == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := s.store.Delete(k); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.store.Delete(k)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.httpListener.Addr()
}
