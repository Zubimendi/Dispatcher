package delivery

import "testing"

func TestSignAndVerify(t *testing.T) {
	secret := "whsec_test"
	payload := []byte(`{"hello":"world"}`)

	sig := Sign(secret, payload)
	if !VerifySignature(secret, payload, sig) {
		t.Fatal("expected valid signature to verify")
	}
	if VerifySignature("wrong_secret", payload, sig) {
		t.Fatal("expected signature to fail verification with wrong secret")
	}
	if VerifySignature(secret, []byte(`{"tampered":true}`), sig) {
		t.Fatal("expected signature to fail verification against tampered payload")
	}
}
