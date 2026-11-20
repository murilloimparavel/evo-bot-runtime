package service

import "testing"

func TestKnowledgePilotScope(t *testing.T) {
	rootA := "11111111-1111-4111-8111-111111111111"
	rootB := "22222222-2222-4222-8222-222222222222"
	t.Setenv("KNOWLEDGE_DISPATCH_ENABLED", "true")
	t.Setenv("KNOWLEDGE_RUNTIME_PILOT_ROOT_IDS", rootA)
	for root, want := range map[string]bool{rootA: true, rootB: false} {
		got, err := knowledgeDispatchSelected("https://processor.test/api/v1/a2a/" + root)
		if err != nil || got != want {
			t.Fatalf("root %s selected=%v want=%v err=%v", root, got, want, err)
		}
	}
	for _, invalid := range []string{"", "bad", rootA + ",", rootA + "," + rootA} {
		t.Setenv("KNOWLEDGE_RUNTIME_PILOT_ROOT_IDS", invalid)
		if _, err := knowledgeDispatchSelected("https://processor.test/api/v1/a2a/" + rootA); err == nil {
			t.Fatalf("invalid list accepted: %q", invalid)
		}
	}
}

func TestKnowledgePilotRejectsAmbiguousRootURL(t *testing.T) {
	t.Setenv("KNOWLEDGE_DISPATCH_ENABLED", "true")
	t.Setenv("KNOWLEDGE_RUNTIME_PILOT_ROOT_IDS", "11111111-1111-4111-8111-111111111111")
	for _, endpoint := range []string{
		"https://processor.test/api/v1/a2a/11111111-1111-4111-8111-111111111111?other=root",
		"https://processor.test/api/v1/a2a/%31%31%31%31%31%31%31%31-1111-4111-8111-111111111111",
		"https://processor.test/other",
	} {
		if _, err := knowledgeDispatchSelected(endpoint); err == nil {
			t.Fatalf("ambiguous endpoint accepted: %s", endpoint)
		}
	}
}
