package httpx

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// BenchmarkReadJSON_Vote измеряет разбор типичного тела голоса —
// самый частый вызов на hot path.
func BenchmarkReadJSON_Vote(b *testing.B) {
	body := []byte(`{"option_id":2}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/polls/p1/vote", bytes.NewReader(body))
		var v model.VoteRequest
		if err := ReadJSON(req, &v); err != nil {
			b.Fatal(err)
		}
		if v.OptionID != 2 {
			b.Fatalf("option_id = %d", v.OptionID)
		}
	}
}
