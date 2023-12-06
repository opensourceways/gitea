package storage

import (
	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/setting"
	"code.gitea.io/gitea/modules/structs"
	"context"
	"encoding/json"
	"fmt"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"net/http"
	"net/url"
	"strconv"
)

const multipart_chunk_size int64 = 20000000
const multipart_default_expire int = 1800

type MultipartPartID struct {
	Etag  string `json:"etag"`
	Index int    `json:"index"`
}

type MultiPartCommitUpload struct {
	UploadID string            `json:"upload_id"`
	PartIDs  []MultipartPartID `json:"part_ids"`
}

// NewHWCloudStorage returns a hwcloud storage
func NewHWCloudStorage(ctx context.Context, cfg *setting.Storage) (ObjectStorage, error) {
	m, err := NewMinioStorage(ctx, cfg)
	if err != nil {
		return nil, err
	}

	obsCfg := &cfg.MinioConfig

	cli, err := obs.New(obsCfg.AccessKeyID, obsCfg.SecretAccessKey, obsCfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("new obs client failed, err:%s", err.Error())
	}

	return &HWCloudStorage{
		hwclient:     cli,
		bucketDomain: cfg.MinioConfig.BucketDomain,
		MinioStorage: m.(*MinioStorage),
	}, nil
}

type HWCloudStorage struct {
	hwclient     *obs.ObsClient
	bucketDomain string

	*MinioStorage
}

func (hwc *HWCloudStorage) GenerateMultipartParts(path string, size int64) (parts []*structs.MultipartObjectPart, abort *structs.MultipartEndpoint, verify *structs.MultipartEndpoint, err error) {
	//1. list all the multipart tasks
	//TODO
	//2. get and return all unfinished tasks, clean up the task if needed
	//TODO
	//3. Initialize multipart task
	log.Trace("lfs[multipart] Starting to create multipart task %s and %s", hwc.bucket, hwc.buildMinioPath(path))
	upload := obs.InitiateMultipartUploadInput{}
	upload.Key = hwc.buildMinioPath(path)
	upload.Bucket = hwc.bucket
	multipart, err := hwc.hwclient.InitiateMultipartUpload(&upload)
	if err != nil {
		return nil, nil, nil, err
	}
	//generate part
	currentPart := int64(0)
	for {
		if currentPart*multipart_chunk_size >= size {
			break
		}
		request := obs.CreateSignedUrlInput{
			Method:  obs.HttpMethodPut,
			Bucket:  hwc.bucket,
			Key:     hwc.buildMinioPath(path),
			Expires: multipart_default_expire,
			QueryParams: map[string]string{
				"partNumber": strconv.FormatInt(currentPart+1, 10),
				"uploadId":   multipart.UploadId,
			},
		}
		result, errorMessage := hwc.hwclient.CreateSignedUrl(&request)
		if errorMessage != nil {
			return nil, nil, nil, err
		}
		partSize := size - currentPart*multipart_chunk_size
		if partSize > multipart_chunk_size {
			partSize = multipart_chunk_size
		}
		var part = &structs.MultipartObjectPart{
			Index: int(currentPart) + 1,
			Pos:   currentPart * multipart_chunk_size,
			Size:  partSize,
			MultipartEndpoint: &structs.MultipartEndpoint{
				ExpiresIn:         multipart_default_expire,
				Href:              result.SignedUrl,
				Method:            http.MethodPut,
				Headers:           nil,
				Params:            nil,
				AggregationParams: nil,
			},
		}
		parts = append(parts, part)
		currentPart += 1
	}
	//generate abort
	//TODO
	//generate verify
	verify = &structs.MultipartEndpoint{
		Params: &map[string]string{
			"upload_id": multipart.UploadId,
		},
		AggregationParams: &map[string]string{
			"key":  "part_ids",
			"type": "array",
			"item": "index,etag",
		},
	}
	return parts, nil, verify, nil
}

func (hwc *HWCloudStorage) CommitUpload(path, additionalParameter string) error {
	var param MultiPartCommitUpload
	err := json.Unmarshal([]byte(additionalParameter), &param)
	if err != nil {
		log.Error("lfs[multipart] unable to decode additional parameter", additionalParameter)
		return err
	}
	//merge multipart
	parts := make([]obs.Part, 0, len(param.PartIDs))
	for _, p := range param.PartIDs {
		parts = append(parts, obs.Part{ETag: p.Etag, PartNumber: p.Index})
	}
	complete := &obs.CompleteMultipartUploadInput{}
	complete.Bucket = hwc.bucket
	complete.Key = hwc.buildMinioPath(path)
	complete.UploadId = param.UploadID
	complete.Parts = parts
	log.Trace("lfs[multipart] Start to merge multipart task %s and %s", hwc.bucket, hwc.buildMinioPath(path))
	_, err = hwc.hwclient.CompleteMultipartUpload(complete)
	if err != nil {
		return err
	}
	//TODO notify CDN to fetch new object
	return nil

}

// URL gets the redirect URL to a file. The presigned link is valid for 5 minutes.
func (hwc *HWCloudStorage) URL(path, name string) (*url.URL, error) {
	input := &obs.CreateSignedUrlInput{}

	input.Method = obs.HttpMethodGet
	input.Bucket = hwc.bucket
	input.Key = hwc.buildMinioPath(path)
	input.Expires = 3600

	output, err := hwc.hwclient.CreateSignedUrl(input)
	if err != nil {
		return nil, err
	}

	//TODO: support presigned CDN rul
	v, err := url.Parse(output.SignedUrl)
	if err == nil {
		v.Host = hwc.bucketDomain
		v.Scheme = "http"
	}

	return v, err
}

func init() {
	RegisterStorageType(setting.HWCloudStorageType, NewMinioStorage)
}
