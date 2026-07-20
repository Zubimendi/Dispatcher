// Package graphql exposes Dispatcher over a GraphQL API using
// github.com/graphql-go/graphql (hand-assembled schema, no codegen step,
// so the schema doubles as documentation you can read top to bottom).
//
// Two principles live here specifically:
//   "If the client provides business-critical data, validate it on the
//    server." - every mutation revalidates inputs even though a real
//    frontend would also validate them; the server is the trust boundary.
//   "If work doesn't require an immediate response, execute it
//    asynchronously." - publishEvent returns as soon as the outbox
//    transaction commits (milliseconds); actual HTTP delivery to
//    customer endpoints happens later, out of band, in the worker.
package graphql

import (
	"github.com/graphql-go/graphql"
)

func BuildSchema(r *Resolvers) (graphql.Schema, error) {
	endpointType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Endpoint",
		Fields: graphql.Fields{
			"id":                  &graphql.Field{Type: graphql.String},
			"url":                 &graphql.Field{Type: graphql.String},
			"eventTypes":          &graphql.Field{Type: graphql.NewList(graphql.String)},
			"isActive":            &graphql.Field{Type: graphql.Boolean},
			"circuitState":        &graphql.Field{Type: graphql.String},
			"circuitFailureCount": &graphql.Field{Type: graphql.Int},
		},
	})

	eventType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Event",
		Fields: graphql.Fields{
			"id":        &graphql.Field{Type: graphql.String},
			"eventType": &graphql.Field{Type: graphql.String},
			"status":    &graphql.Field{Type: graphql.String},
			"createdAt": &graphql.Field{Type: graphql.String},
		},
	})

	deliveryStatusType := graphql.NewObject(graphql.ObjectConfig{
		Name: "DeliveryStatus",
		Fields: graphql.Fields{
			"endpointId":   &graphql.Field{Type: graphql.String},
			"status":       &graphql.Field{Type: graphql.String},
			"attemptCount": &graphql.Field{Type: graphql.Int},
			"lastError":    &graphql.Field{Type: graphql.String},
		},
	})

	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"endpoint": &graphql.Field{
				Type: endpointType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: r.ResolveEndpoint,
			},
			"deliveryStatus": &graphql.Field{
				Type: graphql.NewList(deliveryStatusType),
				Args: graphql.FieldConfigArgument{
					"eventId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: r.ResolveDeliveryStatus,
			},
		},
	})

	mutationType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Mutation",
		Fields: graphql.Fields{
			"createEndpoint": &graphql.Field{
				Type: endpointType,
				Args: graphql.FieldConfigArgument{
					"tenantId":   &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
					"url":        &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
					"eventTypes": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.NewList(graphql.String))},
				},
				Resolve: r.ResolveCreateEndpoint,
			},
			"updateEndpoint": &graphql.Field{
				Type: endpointType,
				Args: graphql.FieldConfigArgument{
					"id":         &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
					"url":        &graphql.ArgumentConfig{Type: graphql.String},
					"isActive":   &graphql.ArgumentConfig{Type: graphql.Boolean},
					"eventTypes": &graphql.ArgumentConfig{Type: graphql.NewList(graphql.String)},
				},
				Resolve: r.ResolveUpdateEndpoint,
			},
			"publishEvent": &graphql.Field{
				Type: eventType,
				Args: graphql.FieldConfigArgument{
					"tenantId":       &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
					"eventType":      &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
					"payload":        &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)}, // raw JSON string
					"idempotencyKey": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: r.ResolvePublishEvent,
			},
		},
	})

	return graphql.NewSchema(graphql.SchemaConfig{Query: queryType, Mutation: mutationType})
}
