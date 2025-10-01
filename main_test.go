package main

import "testing"

func TestVerseBlockParsingAndReplacement(t *testing.T) {
	ch := `Intro line

### 1
In the beginning **God** created the heaven and the earth.

### 2
And the earth was without form, and void; and darkness was upon the face of the deep.

Tail.
`
	blocks, replaced, err := ParseAndReplaceChapter(ch, "Genesis 1")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(blocks))
	}
	if blocks[0].Num != 1 || blocks[1].Num != 2 {
		t.Fatalf("wrong numbers: %+v", blocks)
	}
	if blocks[0].Content == "" || blocks[1].Content == "" {
		t.Fatalf("empty content")
	}
	want := `Intro line

### 1
![[Genesis 1.1#^kjv]]

### 2
![[Genesis 1.2#^kjv]]

Tail.
`
	if replaced != want {
		t.Fatalf("replacement mismatch.\n--want--\n%s\n--got--\n%s", want, replaced)
	}
}

func TestYAMLPreservationAndBodyReplace(t *testing.T) {
	in := `---
id: abc
tags: [verse]
---
Old body
^oldid
`
	fm, _ := SplitYAMLFrontMatter(in)
	if fm == "" {
		t.Fatalf("front matter not detected")
	}
	// replace body and ensure single ^kjv
	newBody := "New verse body.\n"
	newBody = EnsureSingleTrailingBlockID(newBody, "kjv")
	if newBody != "New verse body.\n^kjv\n" {
		t.Fatalf("block id enforce failed: %q", newBody)
	}
	full := fm + newBody
	if full != `---
id: abc
tags: [verse]
---
New verse body.
^kjv
` {
		t.Fatalf("unexpected full content:\n%s", full)
	}
}

func TestLinkRewriting(t *testing.T) {
	alias := map[string]string{
		"H7225": "rêʾšît (beginning)",
		"G3056": "logos",
	}
	in := `See [[H7225]] and [[H7225|whatever]] and [[G3056|word]]. Also [[H9999]] stays.`
	out, count := RewriteLinks(in, alias)
	if count != 3 {
		t.Fatalf("expected 3 rewrites, got %d", count)
	}
	want := `See [[H7225|rêʾšît (beginning)]] and [[H7225|rêʾšît (beginning)]] and [[G3056|logos]]. Also [[H9999]] stays.`
	if out != want {
		t.Fatalf("rewrite mismatch.\nwant: %s\ngot:  %s", want, out)
	}
}

func TestChapterToVerseReplacementOnly(t *testing.T) {
	ch := "### 12\nabc\n\n### 13\nxyz\n"
	_, rep, err := ParseAndReplaceChapter(ch, "Exodus 5")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := "![[Exodus 5.12#^kjv]]\n![[Exodus 5.13#^kjv]]\n"
	if rep != want {
		t.Fatalf("replacement wrong.\nwant:\n%s\ngot:\n%s", want, rep)
	}
}
