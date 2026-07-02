package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// RetryAction is the decision an agent makes after a job fails.
type RetryAction string

const (
	ActionRetry    RetryAction = "retry"    // transient error, worth retrying
	ActionSkip     RetryAction = "skip"     // permanent error, retrying is pointless
	ActionEscalate RetryAction = "escalate" // ambiguous, needs a human
)

// Decider is implemented by RetryAgent (and by fakes in tests) so the
// scheduler never has a hard dependency on a live LLM API call.
type Decider interface {
	Decide(job *Job) RetryAction
}

// RetryAgent uses an LLM to reason about *why* a job failed and decide what
// should happen next, instead of always retrying every error identically
// until MaxRetries is hit. A validation error and a network timeout are not
// the same problem and shouldn't be handled the same way.
type RetryAgent struct {
	apiKey string
	model  string
	client *http.Client
}

func NewRetryAgent() *RetryAgent {
	return &RetryAgent{
		apiKey: os.Getenv("ANTHROPIC_API_KEY"),
		model:  "claude-haiku-4-5-20251001",
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Enabled reports whether the agent has a usable API key. Callers should
// fall back to plain retry behavior when this is false, so the scheduler
// never depends on an external API being available to function correctly.
func (a *RetryAgent) Enabled() bool {
	return a.apiKey != ""
}

type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens"`
	System    string             `json:"system"`
	Messages  []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

const retryAgentSystemPrompt = `You are a job-retry policy agent for a background job scheduler.
Given a job's failure error message and how many times it has been attempted,
decide the next action. Respond with ONLY one word, no punctuation, no explanation:
- "retry" if the error looks transient (timeouts, connection errors, rate limits, temporary unavailability)
- "skip" if the error looks permanent and retrying will never succeed (validation errors, malformed input, not found, permission denied)
- "escalate" if the error is ambiguous, unusual, or looks like it needs human judgement`

// Decide asks the LLM what to do next for a failed job. On any failure to
// reach the API (network error, missing key, bad response) it safely falls
// back to ActionRetry so job processing never silently breaks because an
// external dependency is unavailable.
func (a *RetryAgent) Decide(job *Job) RetryAction {
	if !a.Enabled() {
		return ActionRetry
	}

	userMsg := fmt.Sprintf(
		"Job error: %q\nAttempt number: %d\nMax retries allowed: %d\nWhat should happen next?",
		job.LastError, job.Attempts, job.MaxRetries,
	)

	reqBody := anthropicRequest{
		Model:     a.model,
		MaxTokens: 10,
		System:    retryAgentSystemPrompt,
		Messages: []anthropicMessage{
			{Role: "user", Content: userMsg},
		},
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return ActionRetry
	}

	req, err := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(payload))
	if err != nil {
		return ActionRetry
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", a.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := a.client.Do(req)
	if err != nil {
		return ActionRetry
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ActionRetry
	}

	var ar anthropicResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return ActionRetry
	}
	if len(ar.Content) == 0 {
		return ActionRetry
	}

	switch ar.Content[0].Text {
	case "skip":
		return ActionSkip
	case "escalate":
		return ActionEscalate
	default:
		return ActionRetry
	}
}
