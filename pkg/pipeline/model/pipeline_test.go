package model

import (
	"strings"
	"testing"
)

func TestMessageEventValidateResponseNoticeLimit(t *testing.T) {
	event := validMessageEvent()
	event.ResponseNotice = strings.Repeat("a", 512)
	if err := event.Validate(); err != nil {
		t.Fatalf("512-byte response notice should be accepted: %v", err)
	}

	event.ResponseNotice += "a"
	if err := event.Validate(); err == nil {
		t.Fatal("513-byte response notice should be rejected")
	}
}

func validMessageEvent() *MessageEvent {
	return &MessageEvent{
		ContactID:      1,
		ConversationID: 1,
		MessageID:      "message-1",
		PostbackURL:    "https://example.invalid/postback",
	}
}
