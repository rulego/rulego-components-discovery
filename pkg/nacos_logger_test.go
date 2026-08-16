package discovery

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type capLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (c *capLogger) add(prefix, format string, v ...any) {
	c.mu.Lock()
	c.msgs = append(c.msgs, prefix+fmt.Sprintf(format, v...))
	c.mu.Unlock()
}

func (c *capLogger) Printf(format string, v ...any) { c.add("[P] ", format, v...) }
func (c *capLogger) Debugf(format string, v ...any) { c.add("[D] ", format, v...) }
func (c *capLogger) Infof(format string, v ...any)  { c.add("[I] ", format, v...) }
func (c *capLogger) Warnf(format string, v ...any)  { c.add("[W] ", format, v...) }
func (c *capLogger) Errorf(format string, v ...any) { c.add("[E] ", format, v...) }

func (c *capLogger) list() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.msgs...)
}

func withShortWindow(t *testing.T, d time.Duration) {
	t.Helper()
	old := logSuppressWindow
	logSuppressWindow = d
	t.Cleanup(func() { logSuppressWindow = old })
}

func TestNacosLoggerFirstMessagePasses(t *testing.T) {
	cap := &capLogger{}
	l := newNacosLogger(cap)
	l.Errorf("nacos server down")
	msgs := cap.list()
	if len(msgs) != 1 || msgs[0] != "[E] nacos server down" {
		t.Fatalf("want 1 forwarded error, got %v", msgs)
	}
}

func TestNacosLoggerStripsStackTrace(t *testing.T) {
	cap := &capLogger{}
	l := newNacosLogger(cap)
	l.Errorf("Send request fail, error=client not connected\ngithub.com/x/y.Request\n\t/some/file.go:1")
	msgs := cap.list()
	if len(msgs) != 1 || strings.Contains(msgs[0], "\n") || strings.Contains(msgs[0], "file.go") {
		t.Fatalf("want single line without stack, got %q", msgs)
	}
}

func TestNacosLoggerSuppressesRepeats(t *testing.T) {
	withShortWindow(t, time.Hour)
	cap := &capLogger{}
	l := newNacosLogger(cap)
	for i := range 10 {
		l.Errorf("Send request fail, retryTimes=%d, error=client not connected", i)
	}
	msgs := cap.list()
	if len(msgs) != 1 {
		t.Fatalf("want repeats collapsed to 1, got %v", msgs)
	}
}

func TestNacosLoggerEmitsCountAfterWindow(t *testing.T) {
	withShortWindow(t, 30*time.Millisecond)
	cap := &capLogger{}
	l := newNacosLogger(cap)
	l.Warnf("ConfigBatchListenRequest failure")
	for range 5 {
		l.Warnf("ConfigBatchListenRequest failure")
	}
	time.Sleep(40 * time.Millisecond)
	l.Warnf("ConfigBatchListenRequest failure")
	msgs := cap.list()
	if len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %v", msgs)
	}
	if !strings.Contains(msgs[1], "repeated 5 times") {
		t.Errorf("want suppressed count in %q", msgs[1])
	}
	// counter resets after emission
	time.Sleep(40 * time.Millisecond)
	l.Warnf("ConfigBatchListenRequest failure")
	msgs = cap.list()
	if len(msgs) != 3 || strings.Contains(msgs[2], "repeated") {
		t.Errorf("want clean emission after reset, got %v", msgs)
	}
}

func TestNacosLoggerDistinctMessagesNotSuppressed(t *testing.T) {
	withShortWindow(t, time.Hour)
	cap := &capLogger{}
	l := newNacosLogger(cap)
	l.Errorf("listen dataId=a.json fail")
	l.Errorf("listen dataId=b.json fail")
	l.Infof("connection established")
	if len(cap.list()) != 3 {
		t.Fatalf("want all distinct messages forwarded, got %v", cap.list())
	}
}

// letters renders n as a 3-char base-26 string so messages stay distinct
// after digit normalization.
func letters(n int) string {
	return string(rune('a'+n/676%26)) + string(rune('a'+n/26%26)) + string(rune('a'+n%26))
}

func TestNacosLoggerTableBounded(t *testing.T) {
	withShortWindow(t, time.Hour)
	cap := &capLogger{}
	l := newNacosLogger(cap)
	for i := range logMaxEntries + 2 {
		l.Infof("msg %s", letters(i))
	}
	// table was reset, so an earlier message is no longer suppressed
	l.Infof("msg %s", letters(0))
	if got := len(cap.list()); got != logMaxEntries+3 {
		t.Fatalf("want %d forwarded after reset, got %d", logMaxEntries+3, got)
	}
}
