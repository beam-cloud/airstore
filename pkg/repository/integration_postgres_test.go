package repository

import (
	"context"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	"github.com/beam-cloud/airstore/pkg/types"
)

type collectingEventRecorder struct {
	mu     sync.Mutex
	events []instrumentation.Event
}

func (r *collectingEventRecorder) Record(_ context.Context, event instrumentation.Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *collectingEventRecorder) snapshot() []instrumentation.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]instrumentation.Event, len(r.events))
	copy(out, r.events)
	return out
}

func TestSaveConnectionEmitsCreatedEventOnInsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	recorder := &collectingEventRecorder{}
	backend := &PostgresBackend{db: db, recorder: recorder}

	createdAt := time.Now().UTC()
	updatedAt := createdAt
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO integration_connection")).
		WithArgs(uint(42), (*uint)(nil), "github", sqlmock.AnyArg(), "shared", nil).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "workspace_id", "member_id", "integration_type", "credentials", "scope", "expires_at", "created_at", "updated_at", "inserted",
		}).AddRow(
			1,
			"conn-ext-1",
			42,
			nil,
			"github",
			[]byte(`{"access_token":"x"}`),
			"shared",
			nil,
			createdAt,
			updatedAt,
			true,
		))

	conn, err := backend.SaveConnection(
		context.Background(),
		42,
		nil,
		"github",
		&types.IntegrationCredentials{AccessToken: "x"},
		"shared",
	)
	if err != nil {
		t.Fatalf("SaveConnection returned error: %v", err)
	}
	if conn == nil {
		t.Fatal("SaveConnection returned nil connection")
	}
	if conn.ExternalId != "conn-ext-1" {
		t.Fatalf("expected external id conn-ext-1, got %q", conn.ExternalId)
	}

	events := recorder.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != "connection.created" {
		t.Fatalf("expected event type connection.created, got %q", events[0].Type)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestSaveConnectionSkipsCreatedEventOnUpdate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	recorder := &collectingEventRecorder{}
	backend := &PostgresBackend{db: db, recorder: recorder}

	createdAt := time.Now().UTC()
	updatedAt := createdAt.Add(time.Minute)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO integration_connection")).
		WithArgs(uint(42), (*uint)(nil), "github", sqlmock.AnyArg(), "shared", nil).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "workspace_id", "member_id", "integration_type", "credentials", "scope", "expires_at", "created_at", "updated_at", "inserted",
		}).AddRow(
			1,
			"conn-ext-1",
			42,
			nil,
			"github",
			[]byte(`{"access_token":"x"}`),
			"shared",
			nil,
			createdAt,
			updatedAt,
			false,
		))

	conn, err := backend.SaveConnection(
		context.Background(),
		42,
		nil,
		"github",
		&types.IntegrationCredentials{AccessToken: "x"},
		"shared",
	)
	if err != nil {
		t.Fatalf("SaveConnection returned error: %v", err)
	}
	if conn == nil {
		t.Fatal("SaveConnection returned nil connection")
	}
	if conn.ExternalId != "conn-ext-1" {
		t.Fatalf("expected external id conn-ext-1, got %q", conn.ExternalId)
	}

	events := recorder.snapshot()
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
