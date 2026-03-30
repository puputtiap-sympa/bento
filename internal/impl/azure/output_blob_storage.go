package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// Blob Storage Output Fields
	bsoFieldContainer         = "container"
	bsoFieldPath              = "path"
	bsoFieldBlobType          = "blob_type"
	bsoFieldPublicAccessLevel = "public_access_level"
	bsoFieldBatching          = "batching"
)

// Maximum approximate block size we accumulate before flushing to a single AppendBlock call.
// Keep below Azure limits of ~4MB per AppendBlock; 50KB is a conservative default that
// allows high message counts before hitting the 50,000 block limit.
const maxAppendBufSize = 50 * 1024

type bsoConfig struct {
	client            *azblob.Client
	Container         *service.InterpolatedString
	Path              *service.InterpolatedString
	BlobType          *service.InterpolatedString
	PublicAccessLevel *service.InterpolatedString
}

func bsoConfigFromParsed(pConf *service.ParsedConfig) (conf bsoConfig, err error) {
	if conf.Container, err = pConf.FieldInterpolatedString(bsoFieldContainer); err != nil {
		return
	}
	var containerSASToken bool
	c, err := conf.Container.TryString(service.NewMessage([]byte("")))
	if err != nil {
		return
	}
	if conf.client, containerSASToken, err = blobStorageClientFromParsed(pConf, c); err != nil {
		return
	}
	if containerSASToken {
		// if using a container SAS token, the container is already implicit
		conf.Container, _ = service.NewInterpolatedString("")
	}
	if conf.Path, err = pConf.FieldInterpolatedString(bsoFieldPath); err != nil {
		return
	}
	if conf.BlobType, err = pConf.FieldInterpolatedString(bsoFieldBlobType); err != nil {
		return
	}
	if conf.PublicAccessLevel, err = pConf.FieldInterpolatedString(bsoFieldPublicAccessLevel); err != nil {
		return
	}
	return
}

func bsoSpec() *service.ConfigSpec {
	return azureComponentSpec(true).
		Stable().
		Summary(`Sends message parts as objects to an Azure Blob Storage Account container. Each object is uploaded with the filename specified with the `+"`container`"+` field.`).
		Description(`
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

Supports multiple authentication methods but only one of the following is required:
- `+"`storage_connection_string`"+`
- `+"`storage_account` and `storage_access_key`"+`
- `+"`storage_account` and `storage_sas_token`"+`
- `+"`storage_account` to access via [DefaultAzureCredential](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential)"+`

If multiple are set then the `+"`storage_connection_string`"+` is given priority.

If the `+"`storage_connection_string`"+` does not contain the `+"`AccountName`"+` parameter, please specify it in the
`+"`storage_account`"+` field.

When using `+"`APPEND`"+` blob type, the batching configuration controls how messages are
grouped before being written. Multiple messages in a batch are concatenated into a single
`+"`AppendBlock`"+` call, which reduces the number of blocks consumed toward the 50,000 block
limit per blob. This is recommended for high-throughput append workloads.

When using `+"`BLOCK`"+` blob type (default), each message is uploaded as a separate blob
using `+"`UploadStream`"+`. The batching configuration has no effect on BLOCK blobs as each
message produces an independent blob object.`+service.OutputPerformanceDocs(true, false)).
		Fields(
			service.NewInterpolatedStringField(bsoFieldContainer).
				Description("The container for uploading the messages to.").
				Example(`messages-${!timestamp("2006")}`),
			service.NewInterpolatedStringField(bsoFieldPath).
				Description("The path of each message to upload.").
				Example(`${!count("files")}-${!timestamp_unix_nano()}.json`).
				Example(`${!metadata("kafka_key")}.json`).
				Example(`${!json("doc.namespace")}/${!json("doc.id")}.json`).
				Default(`${!count("files")}-${!timestamp_unix_nano()}.txt`),
			service.NewInterpolatedStringEnumField(bsoFieldBlobType, "BLOCK", "APPEND").
				Description("Block and Append blobs are comprised of blocks, and each blob can support up to 50,000 blocks. When using `+\"`APPEND`\"+` blobs, the batching configuration reduces the number of blocks consumed by concatenating multiple messages into a single `+\"`AppendBlock`\"+` call. When using `+\"`BLOCK`\"+` blobs, each message is uploaded as a separate blob and batching has no effect. The default value is `+\"`BLOCK`\"+`.").
				Advanced().
				Default("BLOCK"),
			service.NewInterpolatedStringEnumField(bsoFieldPublicAccessLevel, "PRIVATE", "BLOB", "CONTAINER").
				Description(`The container's public access level. The default value is `+"`PRIVATE`"+`.`).
				Advanced().
				Default("PRIVATE"),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(bsoFieldBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput("azure_blob_storage", bsoSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(bsoFieldBatching); err != nil {
				return
			}
			var pConf bsoConfig
			if pConf, err = bsoConfigFromParsed(conf); err != nil {
				return
			}

			// Warn if batching is configured but blob_type is BLOCK (batching only benefits APPEND blobs).
			if batchPolicy.Count > 0 || batchPolicy.ByteSize > 0 || batchPolicy.Period != "" {
				if blobType, berr := pConf.BlobType.TryString(service.NewMessage([]byte(""))); berr == nil && blobType == "BLOCK" {
					mgr.Logger().Warn("batching configuration has no effect when blob_type is BLOCK; batching only reduces block count for APPEND blobs")
				}
			}

			out, err = newAzureBlobStorageWriter(pConf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
}

type azureBlobStorageWriter struct {
	conf bsoConfig
	log  *service.Logger
}

// Ensure azureBlobStorageWriter implements BatchOutput
var _ service.BatchOutput = (*azureBlobStorageWriter)(nil)

func newAzureBlobStorageWriter(conf bsoConfig, log *service.Logger) (*azureBlobStorageWriter, error) {
	return &azureBlobStorageWriter{
		conf: conf,
		log:  log,
	}, nil
}

func (a *azureBlobStorageWriter) Connect(ctx context.Context) error {
	return nil
}

func (a *azureBlobStorageWriter) WriteBatch(ctx context.Context, msg service.MessageBatch) error {
	type destKey struct{ container, blob, blobType string }
	type destBuf struct {
		msgs        [][]byte
		byteLen     int
		accessLevel string // captured from first message for this destination
	}
	buffers := make(map[destKey]*destBuf)

	flush := func(d destKey) error {
		db := buffers[d]
		if db == nil || len(db.msgs) == 0 {
			return nil
		}
		if d.blobType == "APPEND" {
			// Concatenate buffered messages into a single AppendBlock call to reduce block count.
			var agg bytes.Buffer
			for _, m := range db.msgs {
				agg.Write(m)
			}
			if err := a.uploadAppendBlock(ctx, d.container, d.blob, agg.Bytes()); err != nil {
				return err
			}
		} else {
			// BLOCK blobs: each message is uploaded as a separate blob via UploadStream.
			for _, m := range db.msgs {
				if err := a.uploadBlockBlob(ctx, d.container, d.blob, m); err != nil {
					return err
				}
			}
		}
		buffers[d] = &destBuf{accessLevel: db.accessLevel}
		return nil
	}

	flushWithRetry := func(d destKey) error {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			if err := flush(d); err != nil {
				lastErr = err
				if isErrorCode(err, bloberror.ContainerNotFound) {
					if cerr := a.createContainer(ctx, d.container, buffers[d].accessLevel); cerr != nil {
						if !isErrorCode(cerr, bloberror.ContainerAlreadyExists) {
							return fmt.Errorf("failed to create container: %w", cerr)
						}
					}
					continue
				}
				if isTransientError(err) {
					continue
				}
				return err
			}
			return nil
		}
		return fmt.Errorf("upload failed after retries: %w", lastErr)
	}

	if err := msg.WalkWithBatchedErrors(func(i int, m *service.Message) error {
		containerName, err := msg.TryInterpolatedString(i, a.conf.Container)
		if err != nil {
			return fmt.Errorf("container interpolation error: %w", err)
		}

		blobName, err := msg.TryInterpolatedString(i, a.conf.Path)
		if err != nil {
			return fmt.Errorf("path interpolation error: %w", err)
		}

		blobType, err := msg.TryInterpolatedString(i, a.conf.BlobType)
		if err != nil {
			return fmt.Errorf("blob type interpolation error: %w", err)
		}

		mBytes, err := m.AsBytes()
		if err != nil {
			return err
		}

		key := destKey{container: containerName, blob: blobName, blobType: blobType}
		if buffers[key] == nil {
			accessLevel, aerr := msg.TryInterpolatedString(i, a.conf.PublicAccessLevel)
			if aerr != nil {
				return fmt.Errorf("access level interpolation error: %w", aerr)
			}
			buffers[key] = &destBuf{accessLevel: accessLevel}
		}

		buffers[key].msgs = append(buffers[key].msgs, mBytes)
		buffers[key].byteLen += len(mBytes)

		// Flush when buffer exceeds threshold. For APPEND blobs this reduces
		// block count; for BLOCK blobs each message is uploaded individually
		// regardless but we still flush to bound memory usage.
		if buffers[key].byteLen >= maxAppendBufSize {
			if err := flushWithRetry(key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Flush remaining buffers with the same retry logic.
	for k := range buffers {
		if err := flushWithRetry(k); err != nil {
			return err
		}
	}

	return nil
}

// uploadAppendBlock appends data to an APPEND blob, creating the blob if it does not exist.
func (a *azureBlobStorageWriter) uploadAppendBlock(ctx context.Context, containerName, blobName string, data []byte) error {
	appendBlobClient := a.conf.client.ServiceClient().NewContainerClient(containerName).NewAppendBlobClient(blobName)
	_, err := appendBlobClient.AppendBlock(ctx, streaming.NopCloser(bytes.NewReader(data)), nil)
	if err != nil {
		if isErrorCode(err, bloberror.BlobNotFound) {
			_, cerr := appendBlobClient.Create(ctx, nil)
			if cerr != nil && !isErrorCode(cerr, bloberror.BlobAlreadyExists) {
				return fmt.Errorf("failed to create append blob: %w", cerr)
			}
			_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(bytes.NewReader(data)), nil)
			if err != nil {
				return fmt.Errorf("failed retrying to append block to blob: %w", err)
			}
		} else {
			return fmt.Errorf("failed to append block to blob: %w", err)
		}
	}
	return nil
}

// uploadBlockBlob uploads a single message as a BLOCK blob using UploadStream.
func (a *azureBlobStorageWriter) uploadBlockBlob(ctx context.Context, containerName, blobName string, message []byte) error {
	_, err := a.conf.client.ServiceClient().
		NewContainerClient(containerName).
		NewBlockBlobClient(blobName).
		UploadStream(ctx, bytes.NewReader(message), nil)
	if err != nil {
		return fmt.Errorf("failed to upload block blob: %w", err)
	}
	return nil
}

func (a *azureBlobStorageWriter) createContainer(ctx context.Context, containerName, accessLevel string) error {
	var opts azblob.CreateContainerOptions
	switch accessLevel {
	case "BLOB":
		accessType := azblob.PublicAccessTypeBlob
		opts.Access = &accessType
	case "CONTAINER":
		accessType := azblob.PublicAccessTypeContainer
		opts.Access = &accessType
	}
	_, err := a.conf.client.CreateContainer(ctx, containerName, &opts)
	return err
}

func (a *azureBlobStorageWriter) Close(context.Context) error {
	return nil
}

func isErrorCode(err error, code bloberror.Code) bool {
	var rerr *azcore.ResponseError
	if ok := errors.As(err, &rerr); ok {
		return rerr.ErrorCode == string(code)
	}

	return false
}

// isTransientError returns true for transient server-side errors.
func isTransientError(err error) bool {
	var rerr *azcore.ResponseError
	if errors.As(err, &rerr) {
		if rerr.StatusCode >= 500 {
			return true
		}
		switch rerr.ErrorCode {
		case "InternalError", "ServerBusy":
			return true
		}
	}
	return false
}
