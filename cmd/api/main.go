// cmd/api is the GraphQL HTTP server. Stateless: all state lives in
// Postgres/Redis, so you can run any number of replicas behind a load
// balancer with no coordination between them (horizontal scalability).
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	graphqlhandler "github.com/graphql-go/handler"
	"go.uber.org/zap"

	"github.com/yourname/dispatcher/internal/cache"
	"github.com/yourname/dispatcher/internal/config"
	"github.com/yourname/dispatcher/internal/db"
	"github.com/yourname/dispatcher/internal/graphql"
	"github.com/yourname/dispatcher/internal/observability"
	"github.com/yourname/dispatcher/internal/outbox"
)

func main() {
	log := observability.NewLogger()
	defer log.Sync()

	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	cancel()
	if err != nil {
		log.Fatal("db connect failed", zap.Error(err))
	}
	defer pool.Close()

	c := cache.New(cfg.RedisAddr)
	store := outbox.NewStore(pool)
	resolvers := graphql.NewResolvers(pool, store, c)

	schema, err := graphql.BuildSchema(resolvers)
	if err != nil {
		log.Fatal("build schema failed", zap.Error(err))
	}

	gqlHandler := graphqlhandler.New(&graphqlhandler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: true, // interactive explorer at / - handy for local dev
	})

	mux := http.NewServeMux()
	mux.Handle("/graphql", gqlHandler)
	mux.Handle("/metrics", observability.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(pingCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		log.Info("api server starting", zap.String("port", cfg.HTTPPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server failed", zap.Error(err))
		}
	}()

	// Graceful shutdown: stop accepting new connections, let in-flight
	// requests finish (bounded by the timeout), then exit. "A system
	// that cannot recover gracefully from failure isn't production
	// ready" applies just as much to planned deploys as unplanned
	// crashes.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", zap.Error(err))
	}
}
