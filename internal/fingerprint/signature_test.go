package fingerprint

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSigner_SignVerifyRoundTrip(t *testing.T) {
	s := NewSigner("secret")

	value := s.Sign("poll-1", "raw-uuid")
	raw, ok := s.Verify("poll-1", value)
	if !ok {
		t.Fatal("valid signature must verify")
	}
	if raw != "raw-uuid" {
		t.Errorf("raw = %q, want raw-uuid", raw)
	}
}

func TestSigner_RejectsTamperedValue(t *testing.T) {
	s := NewSigner("secret")
	value := s.Sign("poll-1", "raw-uuid")

	// Меняем сырой идентификатор, оставляя старую подпись.
	idx := len("raw-uuid")
	tampered := "evil-uuid" + value[idx:]
	if _, ok := s.Verify("poll-1", tampered); ok {
		t.Error("tampered value must not verify")
	}
}

func TestSigner_RejectsWrongPoll(t *testing.T) {
	s := NewSigner("secret")
	value := s.Sign("poll-1", "raw-uuid")

	if _, ok := s.Verify("poll-2", value); ok {
		t.Error("cookie signed for another poll must not verify")
	}
}

func TestSigner_RejectsWrongSecret(t *testing.T) {
	a := NewSigner("secret-a")
	b := NewSigner("secret-b")

	value := a.Sign("poll-1", "raw-uuid")
	if _, ok := b.Verify("poll-1", value); ok {
		t.Error("cookie signed with another secret must not verify")
	}
}

func TestSigner_RejectsMalformed(t *testing.T) {
	s := NewSigner("secret")
	for _, v := range []string{"", "no-separator", ".onlysig", "raw.", ".", "raw."} {
		if _, ok := s.Verify("poll-1", v); ok {
			t.Errorf("malformed value %q must not verify", v)
		}
	}
}

func TestSigner_IssueIsUnique(t *testing.T) {
	s := NewSigner("secret")
	raw1, cookie1 := s.Issue("poll-1")
	raw2, cookie2 := s.Issue("poll-1")

	if raw1 == raw2 || cookie1 == cookie2 {
		t.Error("each Issue must produce a distinct identifier")
	}
	if got, ok := s.Verify("poll-1", cookie1); !ok || got != raw1 {
		t.Error("issued cookie must verify back to its raw id")
	}
}

func TestFromRequest_ValidCookie(t *testing.T) {
	s := NewSigner("secret")
	req := httptest.NewRequest(http.MethodPost, "/polls/p1/vote", nil)
	req.AddCookie(&http.Cookie{Name: CookieName, Value: s.Sign("p1", "raw-1")})

	if got := FromRequest(req, "p1", s); got != "raw-1" {
		t.Errorf("fingerprint = %q, want raw-1", got)
	}
}

func TestFromRequest_NoCookieFallsBack(t *testing.T) {
	s := NewSigner("secret")
	req := httptest.NewRequest(http.MethodPost, "/polls/p1/vote", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	req.Header.Set("User-Agent", "agent")

	want := HashIP("1.2.3.4")
	if got := FromRequest(req, "p1", s); got != want {
		t.Errorf("fingerprint = %q, want fallback %q", got, want)
	}
}

func TestFromRequest_InvalidCookieFallsBack(t *testing.T) {
	s := NewSigner("secret")
	req := httptest.NewRequest(http.MethodPost, "/polls/p1/vote", nil)
	req.AddCookie(&http.Cookie{Name: CookieName, Value: "forged.badsignature"})
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	req.Header.Set("User-Agent", "agent")
	req.RemoteAddr = "1.2.3.4:5678"

	want := HashIP("1.2.3.4")
	if got := FromRequest(req, "p1", s); got != want {
		t.Errorf("fingerprint = %q, want fallback %q", got, want)
	}
}

// Ключевое свойство анти-накрутки: ротация User-Agent не даёт новых
// fingerprint'ов — fallback зависит только от IP.
func TestFromRequest_UARotationDoesNotChangeFallback(t *testing.T) {
	s := NewSigner("secret")

	fp := func(ua string) string {
		req := httptest.NewRequest(http.MethodPost, "/polls/p1/vote", nil)
		req.Header.Set("X-Forwarded-For", "5.6.7.8")
		req.Header.Set("User-Agent", ua)
		return FromRequest(req, "p1", s)
	}

	first := fp("bot-1")
	if got := fp("bot-2"); got != first {
		t.Errorf("UA rotation changed fingerprint: %q -> %q (must be IP-only)", first, got)
	}
	if got := fp(""); got != first {
		t.Errorf("empty UA changed fingerprint: %q -> %q", first, got)
	}
}

func TestClientIP_Precedence(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "9.9.9.9:1111"

	if got := ClientIP(req); got != "9.9.9.9" {
		t.Errorf("RemoteAddr fallback = %q, want 9.9.9.9", got)
	}

	req.Header.Set("X-Real-IP", "8.8.8.8")
	if got := ClientIP(req); got != "8.8.8.8" {
		t.Errorf("X-Real-IP = %q, want 8.8.8.8", got)
	}

	req.Header.Set("X-Forwarded-For", "7.7.7.7, 6.6.6.6")
	if got := ClientIP(req); got != "7.7.7.7" {
		t.Errorf("X-Forwarded-For = %q, want first address 7.7.7.7", got)
	}
}
