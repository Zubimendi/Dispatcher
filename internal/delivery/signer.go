// HMAC request signing so the receiving server can verify a webhook
// actually came from Dispatcher and was not tampered with in transit.
// Also the natural place to document the *consumer*-side idempotency
// contract: every signed request includes the event ID, and receivers are
// told (see docs/PRD.md) to dedupe on it, because at-least-once delivery
// semantics mean the same event can legitimately arrive twice.
package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

func Sign(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifySignature(secret string, payload []byte, signature string) bool {
	expected := Sign(secret, payload)
	return hmac.Equal([]byte(expected), []byte(signature))
}
