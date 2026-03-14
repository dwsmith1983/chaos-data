package aws

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-lambda-go/events"

	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// ProxyHandler processes S3 event notifications by forwarding each
// object through the chaos engine. It is designed to be invoked as
// an AWS Lambda function triggered by S3 bucket notifications.
type ProxyHandler struct {
	engine     *engine.Engine
	transport  *S3Transport
	holdPrefix string
}

// NewProxyHandler creates a ProxyHandler that uses the given engine
// to process objects and the given transport for S3 operations. Keys
// starting with holdPrefix are skipped to avoid recursive processing
// of objects moved to the hold area.
func NewProxyHandler(eng *engine.Engine, transport *S3Transport, holdPrefix string) *ProxyHandler {
	return &ProxyHandler{
		engine:     eng,
		transport:  transport,
		holdPrefix: holdPrefix,
	}
}

// Handle processes an S3 event by iterating over each record, decoding
// the S3 key, skipping held objects, and running the chaos engine
// against each eligible object. Errors from individual records are
// collected and returned as a combined error.
func (h *ProxyHandler) Handle(ctx context.Context, event events.S3Event) error {
	var errs []error

	for _, record := range event.Records {
		key, err := url.PathUnescape(record.S3.Object.Key)
		if err != nil {
			errs = append(errs, fmt.Errorf("decode key %q: %w", record.S3.Object.Key, err))
			continue
		}

		// Skip objects under the hold prefix to prevent recursive
		// processing when the engine moves objects into the hold area.
		if strings.HasPrefix(key, h.holdPrefix) {
			continue
		}

		obj := types.DataObject{
			Key:          key,
			Size:         record.S3.Object.Size,
			LastModified: record.EventTime,
		}

		if _, processErr := h.engine.ProcessObject(ctx, obj); processErr != nil {
			errs = append(errs, fmt.Errorf("process %q: %w", key, processErr))
		}
	}

	return errors.Join(errs...)
}
