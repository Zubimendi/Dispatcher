package graphql

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/graphql-go/graphql"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yourname/dispatcher/internal/cache"
	"github.com/yourname/dispatcher/internal/outbox"
	"github.com/yourname/dispatcher/pkg/models"
)

type Resolvers struct {
	pool  *pgxpool.Pool
	store *outbox.Store
	cache *cache.Cache
}

func NewResolvers(pool *pgxpool.Pool, store *outbox.Store, c *cache.Cache) *Resolvers {
	return &Resolvers{pool: pool, store: store, cache: c}
}

// --- Query resolvers -------------------------------------------------

func (r *Resolvers) ResolveEndpoint(p graphql.ResolveParams) (interface{}, error) {
	id, _ := p.Args["id"].(string)

	// Cache-aside read path: check Redis first, fall back to Postgres,
	// then populate the cache. This is the "reduce latency without
	// sacrificing consistency" principle - consistency is preserved
	// because every write path invalidates this same key (see
	// ResolveUpdateEndpoint below).
	if cached, err := r.cache.GetEndpoint(p.Context, id); err == nil && cached != nil {
		return cached, nil
	}

	var ep models.Endpoint
	err := r.pool.QueryRow(p.Context, `
		SELECT id, tenant_id, url, event_types, is_active, circuit_state, circuit_failure_count, version
		FROM endpoints WHERE id = $1
	`, id).Scan(&ep.ID, &ep.TenantID, &ep.URL, &ep.EventTypes, &ep.IsActive,
		&ep.CircuitState, &ep.CircuitFailureCount, &ep.Version)
	if err != nil {
		return nil, fmt.Errorf("endpoint not found: %w", err)
	}

	_ = r.cache.SetEndpoint(p.Context, ep)
	return ep, nil
}

func (r *Resolvers) ResolveDeliveryStatus(p graphql.ResolveParams) (interface{}, error) {
	eventID, _ := p.Args["eventId"].(string)

	rows, err := r.pool.Query(p.Context, `
		SELECT endpoint_id, status, attempt_count, last_error
		FROM delivery_jobs WHERE event_id = $1
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type status struct {
		EndpointID   string  `json:"endpointId"`
		Status       string  `json:"status"`
		AttemptCount int     `json:"attemptCount"`
		LastError    *string `json:"lastError"`
	}
	var out []status
	for rows.Next() {
		var s status
		if err := rows.Scan(&s.EndpointID, &s.Status, &s.AttemptCount, &s.LastError); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// --- Mutation resolvers ------------------------------------------------

func (r *Resolvers) ResolveCreateEndpoint(p graphql.ResolveParams) (interface{}, error) {
	tenantID, _ := p.Args["tenantId"].(string)
	rawURL, _ := p.Args["url"].(string)
	eventTypesArg, _ := p.Args["eventTypes"].([]interface{})

	// Server-side validation: the client is not a trusted execution
	// environment. A frontend that skips this check, a malicious API
	// caller, or a buggy internal service must all be stopped here.
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, fmt.Errorf("invalid url: must be a valid absolute http(s) url")
	}
	if len(eventTypesArg) == 0 {
		return nil, fmt.Errorf("eventTypes must contain at least one event type")
	}
	eventTypes := make([]string, 0, len(eventTypesArg))
	for _, et := range eventTypesArg {
		s, ok := et.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return nil, fmt.Errorf("eventTypes must be non-empty strings")
		}
		eventTypes = append(eventTypes, s)
	}

	secret := generateSecret()

	var ep models.Endpoint
	err = r.pool.QueryRow(p.Context, `
		INSERT INTO endpoints (tenant_id, url, secret, event_types)
		VALUES ($1, $2, $3, $4)
		RETURNING id, tenant_id, url, event_types, is_active, circuit_state, circuit_failure_count, version
	`, tenantID, rawURL, secret, eventTypes).Scan(&ep.ID, &ep.TenantID, &ep.URL, &ep.EventTypes,
		&ep.IsActive, &ep.CircuitState, &ep.CircuitFailureCount, &ep.Version)
	if err != nil {
		return nil, fmt.Errorf("create endpoint: %w", err)
	}
	return ep, nil
}

func (r *Resolvers) ResolveUpdateEndpoint(p graphql.ResolveParams) (interface{}, error) {
	id, _ := p.Args["id"].(string)

	setClauses := []string{}
	args := []interface{}{}
	argN := 1

	if rawURL, ok := p.Args["url"].(string); ok {
		if _, err := url.ParseRequestURI(rawURL); err != nil {
			return nil, fmt.Errorf("invalid url")
		}
		argN++
		setClauses = append(setClauses, fmt.Sprintf("url = $%d", argN))
		args = append(args, rawURL)
	}
	if isActive, ok := p.Args["isActive"].(bool); ok {
		argN++
		setClauses = append(setClauses, fmt.Sprintf("is_active = $%d", argN))
		args = append(args, isActive)
	}
	if eventTypesArg, ok := p.Args["eventTypes"].([]interface{}); ok {
		eventTypes := make([]string, 0, len(eventTypesArg))
		for _, et := range eventTypesArg {
			if s, ok := et.(string); ok {
				eventTypes = append(eventTypes, s)
			}
		}
		argN++
		setClauses = append(setClauses, fmt.Sprintf("event_types = $%d", argN))
		args = append(args, eventTypes)
	}

	if len(setClauses) == 0 {
		return nil, fmt.Errorf("no fields to update")
	}

	query := fmt.Sprintf(`
		UPDATE endpoints SET %s, updated_at = now(), version = version + 1
		WHERE id = $1
		RETURNING id, tenant_id, url, event_types, is_active, circuit_state, circuit_failure_count, version
	`, strings.Join(setClauses, ", "))

	fullArgs := append([]interface{}{id}, args...)

	var ep models.Endpoint
	err := r.pool.QueryRow(p.Context, query, fullArgs...).Scan(&ep.ID, &ep.TenantID, &ep.URL,
		&ep.EventTypes, &ep.IsActive, &ep.CircuitState, &ep.CircuitFailureCount, &ep.Version)
	if err != nil {
		return nil, fmt.Errorf("update endpoint: %w", err)
	}

	// Invalidate-on-write: this is what keeps the cache-aside read path
	// correct. Must happen before returning success to the caller.
	if err := r.cache.InvalidateEndpoint(p.Context, id); err != nil {
		return nil, fmt.Errorf("update succeeded but cache invalidation failed: %w", err)
	}

	return ep, nil
}

func (r *Resolvers) ResolvePublishEvent(p graphql.ResolveParams) (interface{}, error) {
	tenantID, _ := p.Args["tenantId"].(string)
	eventType, _ := p.Args["eventType"].(string)
	payloadStr, _ := p.Args["payload"].(string)
	idempotencyKey, _ := p.Args["idempotencyKey"].(string)

	if strings.TrimSpace(eventType) == "" {
		return nil, fmt.Errorf("eventType is required")
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("idempotencyKey is required")
	}
	if !json.Valid([]byte(payloadStr)) {
		return nil, fmt.Errorf("payload must be valid JSON")
	}

	// This call returns as soon as the outbox transaction commits.
	// Nothing here waits on an outbound HTTP call to a customer's
	// server - that happens later, asynchronously, in the worker. This
	// is what keeps p99 latency for publishEvent measured in
	// milliseconds regardless of how slow or flaky subscriber endpoints
	// are.
	event, _, err := r.store.PublishEvent(p.Context, outbox.PublishInput{
		TenantID:       tenantID,
		EventType:      eventType,
		Payload:        json.RawMessage(payloadStr),
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return nil, fmt.Errorf("publish event: %w", err)
	}
	return event, nil
}

func generateSecret() string {
	return "whsec_" + randomHex(32)
}
