package nacos

import (
	"net/textproto"

	"github.com/rulego/rulego/api/types"
)

// dataIdKey is the header/metadata key for dataId.
const dataIdKey = "dataId"

// RequestMessage represents a config-change request message.
type RequestMessage struct {
	dataId string
	body   []byte
	msg    *types.RuleMsg
	err    error
}

func (r *RequestMessage) Body() []byte { return r.body }
func (r *RequestMessage) Headers() textproto.MIMEHeader {
	h := make(textproto.MIMEHeader)
	h.Set(dataIdKey, r.dataId)
	return h
}
func (r *RequestMessage) From() string            { return r.dataId }
func (r *RequestMessage) GetParam(string) string  { return "" }
func (r *RequestMessage) SetMsg(m *types.RuleMsg) { r.msg = m }
func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		m := types.NewMsg(0, r.dataId, types.JSON, types.NewMetadata(), string(r.body))
		m.Metadata.PutValue(dataIdKey, r.dataId)
		r.msg = &m
	}
	return r.msg
}
func (r *RequestMessage) SetStatusCode(int) {}
func (r *RequestMessage) SetBody(b []byte)  { r.body = b }
func (r *RequestMessage) SetError(e error)  { r.err = e }
func (r *RequestMessage) GetError() error   { return r.err }

// ResponseMessage holds the response; config listen has no response path, so it is a stub.
type ResponseMessage struct {
	body []byte
	msg  *types.RuleMsg
	err  error
}

func (r *ResponseMessage) Body() []byte { return r.body }
func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	return make(textproto.MIMEHeader)
}
func (r *ResponseMessage) From() string            { return "" }
func (r *ResponseMessage) GetParam(string) string  { return "" }
func (r *ResponseMessage) SetMsg(m *types.RuleMsg) { r.msg = m }
func (r *ResponseMessage) GetMsg() *types.RuleMsg  { return r.msg }
func (r *ResponseMessage) SetStatusCode(int)       {}
func (r *ResponseMessage) SetBody(b []byte)        { r.body = b }
func (r *ResponseMessage) SetError(e error)        { r.err = e }
func (r *ResponseMessage) GetError() error         { return r.err }
