package discovery

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	nacoslog "github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/rulego/rulego/api/types"
)

// logSuppressWindow is how long repeats of the same message are held back
// before the next occurrence is logged again.
var logSuppressWindow = time.Minute

// logDigitRe normalizes numbers so messages differing only in counters,
// ids or ports share one dedup signature.
var logDigitRe = regexp.MustCompile(`[0-9]+`)

// logMaxEntries bounds the dedup table; it is reset when exceeded.
const logMaxEntries = 1024

// nacosLogger routes nacos SDK logs into rulego's types.Logger.
// While the server is unreachable the SDK emits reconnect-retry errors every
// few seconds with full stack traces; repeats of the same message are
// collapsed to one entry per logSuppressWindow to avoid flooding the log.
type nacosLogger struct {
	log types.Logger

	mu   sync.Mutex
	seen map[string]*logSeenEntry
}

type logSeenEntry struct {
	suppressed int
	lastEmit   time.Time
}

func newNacosLogger(log types.Logger) *nacosLogger {
	return &nacosLogger{log: log, seen: map[string]*logSeenEntry{}}
}

func (l *nacosLogger) Info(args ...any)  { l.emit(l.log.Infof, "%s", fmt.Sprint(args...)) }
func (l *nacosLogger) Warn(args ...any)  { l.emit(l.log.Warnf, "%s", fmt.Sprint(args...)) }
func (l *nacosLogger) Error(args ...any) { l.emit(l.log.Errorf, "%s", fmt.Sprint(args...)) }
func (l *nacosLogger) Debug(args ...any) { l.emit(l.log.Debugf, "%s", fmt.Sprint(args...)) }

func (l *nacosLogger) Infof(format string, args ...any) {
	l.emit(l.log.Infof, format, args...)
}
func (l *nacosLogger) Warnf(format string, args ...any) {
	l.emit(l.log.Warnf, format, args...)
}
func (l *nacosLogger) Errorf(format string, args ...any) {
	l.emit(l.log.Errorf, format, args...)
}
func (l *nacosLogger) Debugf(format string, args ...any) {
	l.emit(l.log.Debugf, format, args...)
}

// emit formats the message, drops any embedded stack trace (everything after
// the first newline) and suppresses repeats within logSuppressWindow. The
// first repeat after the window is logged with the suppressed count appended.
func (l *nacosLogger) emit(logf func(string, ...any), format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	key := logDigitRe.ReplaceAllString(msg, "#")
	l.mu.Lock()
	if len(l.seen) > logMaxEntries {
		l.seen = map[string]*logSeenEntry{}
	}
	now := time.Now()
	e := l.seen[key]
	if e != nil && now.Sub(e.lastEmit) < logSuppressWindow {
		e.suppressed++
		l.mu.Unlock()
		return
	}
	if e == nil {
		e = &logSeenEntry{}
		l.seen[key] = e
	}
	suppressed := e.suppressed
	e.suppressed = 0
	e.lastEmit = now
	l.mu.Unlock()
	if suppressed > 0 {
		msg = fmt.Sprintf("%s (repeated %d times since last log)", msg, suppressed)
	}
	logf("%s", msg)
}

// Close is a no-op.
func (l *nacosLogger) Close() error { return nil }

var _ nacoslog.Logger = (*nacosLogger)(nil)
