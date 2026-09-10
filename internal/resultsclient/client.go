// Package resultsclient — HTTP-клиент к внутреннему API сервиса результатов.
//
// Используется двумя сервисами:
//   - API-воркером: получить метаданные опроса (в частности ends_at), чтобы
//     отклонять голоса после закрытия и вовремя отправлять "done";
//   - consumer'ом: получить ends_at и выполнить идемпотентный flush результатов.
package resultsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// ErrNotFound возвращается, когда опрос не найден (HTTP 404).
type ErrNotFound struct{ PollID string }

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("poll %q not found", e.PollID)
}

// Client — интерфейс, от которого зависят API-воркер и consumer.
type Client interface {
	// GetPoll возвращает метаданные опроса по id.
	GetPoll(ctx context.Context, id string) (model.Poll, error)
	// FlushResults идемпотентно сохраняет результаты опроса.
	// Возвращает true, если результаты записаны этим вызовом, и false, если
	// опрос уже был завершён ранее (повторный flush отклонён).
	FlushResults(ctx context.Context, pollID string, results model.Results) (bool, error)
}

// HTTPClient — реализация Client поверх net/http.
type HTTPClient struct {
	baseURL string
	http    *http.Client
}

// NewHTTPClient создаёт клиент к сервису результатов по адресу baseURL
// (например, "http://results:8081").
func NewHTTPClient(baseURL string, timeout time.Duration) *HTTPClient {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &HTTPClient{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// GetPoll выполняет GET /internal/polls/{id}.
func (c *HTTPClient) GetPoll(ctx context.Context, id string) (model.Poll, error) {
	var poll model.Poll
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/internal/polls/"+id, nil)
	if err != nil {
		return poll, fmt.Errorf("new request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return poll, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		if err := json.NewDecoder(resp.Body).Decode(&poll); err != nil {
			return poll, fmt.Errorf("decode response: %w", err)
		}
		return poll, nil
	case http.StatusNotFound:
		return poll, ErrNotFound{PollID: id}
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return poll, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}
}

// FlushResults выполняет POST /internal/polls/{id}/results.
func (c *HTTPClient) FlushResults(ctx context.Context, pollID string, results model.Results) (bool, error) {
	body, err := json.Marshal(model.FlushRequest{Results: results})
	if err != nil {
		return false, fmt.Errorf("marshal flush request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/internal/polls/"+pollID+"/results", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// Результаты записаны этим вызовом.
		return true, nil
	case http.StatusConflict:
		// Опрос уже завершён — идемпотентный no-op.
		return false, nil
	case http.StatusNotFound:
		return false, ErrNotFound{PollID: pollID}
	default:
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, respBody)
	}
}
