package server

import (
	"encoding/json"
	"errors"
	"github.com/go-chi/chi/v5"
	"net/http"
)

type httpServer struct {
	Log *Log
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func NewHttpServer(addr string) *http.Server {
	server := newHTTPServer()
	router := chi.NewRouter()

	router.Post("/", server.handleProduce)
	router.Get("/", server.handleConsume)

	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

func (server *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, appendErr := server.Log.Append(req.Record)
	if appendErr != nil {
		http.Error(w, appendErr.Error(), http.StatusInternalServerError)
		return
	}

	response := ProduceResponse{
		Offset: offset,
	}
	encodeError := json.NewEncoder(w).Encode(&response)
	if encodeError != nil {
		http.Error(w, appendErr.Error(), http.StatusInternalServerError)
		return
	}
}

func (server *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var request ConsumeRequest
	parseErr := json.NewDecoder(r.Body).Decode(&request)
	if parseErr != nil {
		http.Error(w, parseErr.Error(), http.StatusBadRequest)
		return
	}

	record, readErr := server.Log.Read(request.Offset)
	if errors.Is(readErr, ErrOffsetNotFound) {
		http.Error(w, readErr.Error(), http.StatusNotFound)
		return
	}

	if readErr != nil {
		http.Error(w, readErr.Error(), http.StatusInternalServerError)
		return
	}

	consumeResponse := ConsumeResponse{Record: record}
	encodeErr := json.NewEncoder(w).Encode(consumeResponse)
	if encodeErr != nil {
		http.Error(w, encodeErr.Error(), http.StatusInternalServerError)
		return
	}
}
