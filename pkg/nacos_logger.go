package discovery

import (
	"fmt"

	nacoslog "github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/rulego/rulego/api/types"
)

// nacosLogger routes nacos SDK logs into rulego's types.Logger.
type nacosLogger struct {
	log types.Logger
}

func (l *nacosLogger) Info(args ...interface{})  { l.log.Infof("%s", fmt.Sprint(args...)) }
func (l *nacosLogger) Warn(args ...interface{})  { l.log.Warnf("%s", fmt.Sprint(args...)) }
func (l *nacosLogger) Error(args ...interface{}) { l.log.Errorf("%s", fmt.Sprint(args...)) }
func (l *nacosLogger) Debug(args ...interface{}) { l.log.Debugf("%s", fmt.Sprint(args...)) }

func (l *nacosLogger) Infof(format string, args ...interface{}) {
	l.log.Infof(format, args...)
}
func (l *nacosLogger) Warnf(format string, args ...interface{}) {
	l.log.Warnf(format, args...)
}
func (l *nacosLogger) Errorf(format string, args ...interface{}) {
	l.log.Errorf(format, args...)
}
func (l *nacosLogger) Debugf(format string, args ...interface{}) {
	l.log.Debugf(format, args...)
}

// Close is a no-op.
func (l *nacosLogger) Close() error { return nil }

var _ nacoslog.Logger = (*nacosLogger)(nil)
