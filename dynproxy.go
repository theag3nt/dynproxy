package dynproxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

type directorFunc func(r *http.Request, body []byte) (*url.URL, bool)
type extractorFunc func(r *http.Request, body []byte) (string, bool)
type transformerFunc func(r io.Reader, w io.Writer)

type DynProxy struct {
	Director directorFunc
	// Transformer transformerFunc

	proxy *httputil.ReverseProxy
}

func New(director directorFunc) *DynProxy {
	d := &DynProxy{
		Director: director,
	}
	d.proxy = &httputil.ReverseProxy{
		Director: d.proxyDirector,
	}
	return d
}

func storageDirector(storage Storage, extractor extractorFunc) directorFunc {
	return func(r *http.Request, body []byte) (*url.URL, bool) {
		// // Clone request so the extractor is not able to modify it
		// req := r.Clone(context.Background())

		key, ok := extractor(r, body)
		if !ok {
			return nil, false
		}
		endpoint, ok := storage.Load(key)
		if !ok {
			return nil, false
		}
		return endpoint, true
	}
}

func NewWithStorage(storage Storage, extractor extractorFunc) *DynProxy {
	return New(storageDirector(storage, extractor))
}

func (d *DynProxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if d.proxy == nil {
		panic("") // TODO
	}
	d.proxy.ServeHTTP(rw, req)
}

func (d *DynProxy) ListenAndServe(addr string) {
	if d.proxy == nil {
		panic("") // TODO
	}
	http.ListenAndServe(addr, d.proxy)
}

func (d *DynProxy) proxyDirector(req *http.Request) {
	// Buffer request body, storing errors during read to notify the director
	// There is no way to short-circuit requests before reaching the Transport using ReverseProxy
	// so we want to let the Director handle them (e.g. by setting the req.URL to nil)
	body, err := readBody(req.Body)
	if err != nil {
		d.abort(req)
		return
	}

	// Clear request body so we know if Director rewrites it
	req.Body = nil

	// Direct request to the correct destination
	target, ok := d.Director(req, body)
	if !ok {
		d.abort(req)
		return
	}

	// If Director returned an URL, rewrite the request URL
	if target != nil {
		rewriteRequestUrl(req, target)
	}

	// Block default User-Agent header
	blockDefaultUserAgent(req)

	// Replace request body with the original if it wasn't replaced yet inside Director
	if req.Body == nil {
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
}

func (d *DynProxy) abort(req *http.Request) {
	// No other way to abort the request in ReverseProxy, this will close
	// the connection without a response or logged error (a feature of http.conn)
	panic(http.ErrAbortHandler)
}

var _ http.Handler = &DynProxy{}

func readBody(b io.ReadCloser) ([]byte, error) {
	if b == nil || b == http.NoBody {
		return nil, nil
	}

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(b); err != nil {
		return nil, err
	}
	if err := b.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Taken over from httputil.NewSingleHostReverseProxy()
func rewriteRequestUrl(req *http.Request, target *url.URL) {
	targetQuery := target.RawQuery

	req.URL.Scheme = target.Scheme
	req.URL.Host = target.Host
	req.URL.Path, req.URL.RawPath = joinURLPath(target, req.URL)
	if targetQuery == "" || req.URL.RawQuery == "" {
		req.URL.RawQuery = targetQuery + req.URL.RawQuery
	} else {
		req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
	}
}

// Taken over from httputil.NewSingleHostReverseProxy()
func blockDefaultUserAgent(req *http.Request) {
	// Explicitly disable User-Agent so it's not set to default value
	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header.Set("User-Agent", "")
	}
}

// Taken over from httputil
func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

// Taken over from httputil
func joinURLPath(a, b *url.URL) (path, rawpath string) {
	if a.RawPath == "" && b.RawPath == "" {
		return singleJoiningSlash(a.Path, b.Path), ""
	}
	// Same as singleJoiningSlash, but uses EscapedPath to determine
	// whether a slash should be added
	apath := a.EscapedPath()
	bpath := b.EscapedPath()

	aslash := strings.HasSuffix(apath, "/")
	bslash := strings.HasPrefix(bpath, "/")

	switch {
	case aslash && bslash:
		return a.Path + b.Path[1:], apath + bpath[1:]
	case !aslash && !bslash:
		return a.Path + "/" + b.Path, apath + "/" + bpath
	}
	return a.Path + b.Path, apath + bpath
}
