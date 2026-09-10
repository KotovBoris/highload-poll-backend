package fingerprint

import "testing"

// BenchmarkSign_Issue — выдача cookie (GET /polls/{id}).
func BenchmarkSign_Issue(b *testing.B) {
	s := NewSigner("bench-secret")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Issue("poll-1234567890")
	}
}

// BenchmarkVerify — проверка подписи на голосовании (POST /vote), hot path.
func BenchmarkVerify(b *testing.B) {
	s := NewSigner("bench-secret")
	val := s.Sign("poll-1234567890", "abcdef01-2345-6789-abcd-ef0123456789")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := s.Verify("poll-1234567890", val); !ok {
			b.Fatal("verify failed")
		}
	}
}
