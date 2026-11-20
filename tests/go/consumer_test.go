package consumer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

const fixtureEndpoint = "http://127.0.0.1:8080"
const fixtureProject = "test_project"
const fixtureQuota = "go-batch"

func TestGoConsumerUpgrade(t *testing.T) {
	waitForEmulator(t)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	cfg := odps.NewConfig()
	cfg.Endpoint = fixtureEndpoint + "/api"
	cfg.ProjectName = fixtureProject
	cfg.AccessId = "fixture-go-sdk"
	cfg.AccessKey = "local-go-sdk-fixture"
	cfg.TunnelQuotaName = fixtureQuota
	cfg.HttpTimeout = 8 * time.Second
	service := odps.NewOdps(account.NewAliyunAccount(cfg.AccessId, cfg.AccessKey), cfg.Endpoint)
	service.SetDefaultProjectName(fixtureProject)
	service.SetHttpTimeout(cfg.HttpTimeout)
	db, err := sql.Open("odps", cfg.FormatDsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	table := fmt.Sprintf("go_consumer_upgrade_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, "create table "+table+" (id bigint, note string)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := db.ExecContext(ctx, "drop table "+table); err != nil {
			t.Error(err)
		}
	})

	transfer := tunnel.NewTunnel(service)
	transfer.SetHttpTimeout(cfg.HttpTimeout)
	transfer.SetQuotaName(fixtureQuota)
	options := []tunnel.Option{tunnel.SessionCfg.WithDefaultDeflateCompressor()}
	upload, err := transfer.CreateUploadSession(fixtureProject, table, options...)
	if err != nil {
		t.Fatal(err)
	}
	first := data.Record{data.BigInt(7), data.String("first")}
	second := data.Record{data.BigInt(9), data.String("second")}
	putBlock(t, upload, 3, first)
	resumed, err := transfer.AttachToExistedUploadSession(fixtureProject, table, upload.Id, options...)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(resumed.BlockIds(), []int{3}) {
		t.Fatalf("unexpected durable block list %v", resumed.BlockIds())
	}
	putBlock(t, resumed, 4, second)
	if err := resumed.Commit([]int{3, 4}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := transfer.CreateDownloadSession(fixtureProject, table, options...)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := snapshot.OpenRecordReader(0, snapshot.RecordCount(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var records []data.Record
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if !reflect.DeepEqual(records, []data.Record{first, second}) {
		t.Fatalf("unexpected Protobuf result %v", records)
	}

	rows, err := db.QueryContext(ctx, "select id,note from "+table+" where id >= ? order by id limit 10;", int64(7))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type row struct {
		ID   int64
		Note string
	}
	var found []row
	for rows.Next() {
		var next row
		if err := rows.Scan(&next.ID, &next.Note); err != nil {
			t.Fatal(err)
		}
		found = append(found, next)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(found, []row{{7, "first"}, {9, "second"}}) {
		t.Fatalf("unexpected database/sql result %v", found)
	}
}

func putBlock(t *testing.T, session *tunnel.UploadSession, block int, record data.Record) {
	t.Helper()
	writer, err := session.OpenRecordWriter(block)
	if err != nil {
		t.Fatal(err)
	}
	writeErr := writer.Write(record)
	if err := errors.Join(writeErr, writer.Close()); err != nil {
		t.Fatal(err)
	}
}

func waitForEmulator(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		res, err := client.Get(fixtureEndpoint + "/readyz")
		if err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("isolated Emulator did not become ready")
}
