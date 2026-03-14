package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
)

func main() {
	pipelineBucket := os.Getenv("PIPELINE_BUCKET")
	stagingBucket := os.Getenv("STAGING_BUCKET")
	tableName := os.Getenv("TABLE_NAME")

	if pipelineBucket == "" || stagingBucket == "" || tableName == "" {
		log.Fatal("PIPELINE_BUCKET, STAGING_BUCKET, and TABLE_NAME env vars are required")
	}

	cfg := chaosaws.Config{
		StagingBucket:  stagingBucket,
		PipelineBucket: pipelineBucket,
		TableName:      tableName,
	}
	cfg.Defaults()

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}

	s3Client := s3.NewFromConfig(awsCfg)

	transport := chaosaws.NewS3Transport(s3Client, cfg)
	handler := chaosaws.NewReleaseHandler(transport, s3Client, pipelineBucket, cfg.HoldPrefix)

	fmt.Println("starting release handler lambda")
	lambda.Start(handler.Handle)
}
