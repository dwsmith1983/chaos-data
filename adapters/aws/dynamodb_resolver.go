package aws

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
)

// Compile-time interface assertion.
var _ adapter.DependencyResolver = (*DynamoDBDependencyResolver)(nil)

// maxBFSDepth limits the transitive dependency walk to prevent runaway
// queries against deeply nested or misconfigured dependency graphs.
const maxBFSDepth = 10

// DynamoDBDependencyResolver implements adapter.DependencyResolver by
// querying dependency records from a DynamoDB table. Records use
// PK=DEPS#<pipeline> and SK=DOWNSTREAM#<downstream>. The resolver
// performs a BFS transitive walk to find all downstream pipelines.
type DynamoDBDependencyResolver struct {
	api       DynamoDBAPI
	tableName string
}

// NewDynamoDBDependencyResolver creates a DynamoDBDependencyResolver
// backed by the given DynamoDBAPI client and table name.
func NewDynamoDBDependencyResolver(api DynamoDBAPI, tableName string) *DynamoDBDependencyResolver {
	return &DynamoDBDependencyResolver{
		api:       api,
		tableName: tableName,
	}
}

// GetDownstream returns all transitively downstream pipelines for the
// given target. It performs a BFS walk up to maxBFSDepth levels deep,
// using a visited set for cycle protection. The original target is
// excluded from results. Returns nil when no dependencies are found.
//
// Fail-open: on query error the walk stops and returns whatever has
// been collected so far (consistent with engine fail-open behavior).
func (r *DynamoDBDependencyResolver) GetDownstream(ctx context.Context, target string) ([]string, error) {
	visited := map[string]struct{}{target: {}}
	queue := []string{target}
	var result []string

	for depth := 0; depth < maxBFSDepth && len(queue) > 0; depth++ {
		var nextQueue []string
		for _, pipeline := range queue {
			downstreams, err := r.queryDownstream(ctx, pipeline)
			if err != nil {
				// Fail-open: return what we have so far.
				if len(result) == 0 {
					return nil, nil
				}
				return result, nil
			}
			for _, ds := range downstreams {
				if _, seen := visited[ds]; seen {
					continue
				}
				visited[ds] = struct{}{}
				result = append(result, ds)
				nextQueue = append(nextQueue, ds)
			}
		}
		queue = nextQueue
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// queryDownstream queries the DynamoDB table for all downstream
// dependencies of the given pipeline. Returns the pipeline names
// (with the DOWNSTREAM# prefix stripped).
func (r *DynamoDBDependencyResolver) queryDownstream(ctx context.Context, pipeline string) ([]string, error) {
	pk := DepsPK(pipeline)
	skPrefix := DownstreamSKPrefix()

	out, err := r.api.Query(ctx, &dynamodb.QueryInput{
		TableName:              strPtr(r.tableName),
		KeyConditionExpression: strPtr("PK = :pk AND begins_with(SK, :skPrefix)"),
		ExpressionAttributeValues: map[string]dtypes.AttributeValue{
			":pk":       &dtypes.AttributeValueMemberS{Value: pk},
			":skPrefix": &dtypes.AttributeValueMemberS{Value: skPrefix},
		},
	})
	if err != nil {
		return nil, err
	}

	var result []string
	for _, item := range out.Items {
		sk, ok := stringAttr(item, "SK")
		if !ok {
			continue
		}
		downstream := strings.TrimPrefix(sk, skPrefix)
		if downstream != "" {
			result = append(result, downstream)
		}
	}
	return result, nil
}

// strPtr returns a pointer to the given string.
func strPtr(s string) *string {
	return &s
}
