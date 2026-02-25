package apiv1

import (
	_ "embed"
	"encoding/base64"
	"html/template"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

const (
	pageTitle        = "Airstore"
	pageTitleSuccess = "Connected"
	pageTitleError   = "Connection Failed"
)

//go:embed static/logo.svg
var logoSVG string

// logoDataURI is a data:image/svg+xml;base64,... URI built at init from the embedded SVG.
var logoDataURI string

var (
	errorTemplate   *template.Template
	successTemplate *template.Template
)

func init() {
	// Encode the SVG as a base64 data URI so it can be used in an <img> tag
	// without html/template escaping or parsing issues.
	logoDataURI = "data:image/svg+xml;base64," + base64.StdEncoding.EncodeToString([]byte(strings.TrimSpace(logoSVG)))

	// Inject the data URI into templates before parsing.
	errHTML := strings.ReplaceAll(errorHTMLTmpl, "{{LOGO_URI}}", logoDataURI)
	sucHTML := strings.ReplaceAll(successHTMLTmpl, "{{LOGO_URI}}", logoDataURI)
	errorTemplate = template.Must(template.New("error").Parse(errHTML))
	successTemplate = template.Must(template.New("success").Parse(sucHTML))
}

type errorPageData struct {
	Title   string
	Message string
}

type successPageData struct {
	Title           string
	IntegrationType string
}

func renderErrorPage(c echo.Context, message string) error {
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextHTMLCharsetUTF8)
	c.Response().WriteHeader(http.StatusBadRequest)
	return errorTemplate.Execute(c.Response(), errorPageData{
		Title:   pageTitleError,
		Message: message,
	})
}

func renderSuccessPage(c echo.Context, integrationType string) error {
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextHTMLCharsetUTF8)
	c.Response().WriteHeader(http.StatusOK)
	return successTemplate.Execute(c.Response(), successPageData{
		Title:           pageTitleSuccess,
		IntegrationType: integrationType,
	})
}

// closeScript tries window.close() first. If the browser blocks it (tab not
// opened by JS), it blanks the page and shows a hint to use the keyboard shortcut.
const closeScript = `function tryClose(){window.close();setTimeout(function(){var mac=navigator.platform.indexOf('Mac')>-1;document.body.innerHTML='<div style="display:flex;align-items:center;justify-content:center;height:100vh;font-family:-apple-system,BlinkMacSystemFont,sans-serif;color:#8f8f8f;font-size:14px">'+(mac?'Press \u2318W to close this tab':'Press Ctrl+W to close this tab')+'</div>'},200)}`

const baseStyles = `
	*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
	html { color-scheme: light; }
	body {
		font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
		line-height: 1.6;
		min-height: 100vh;
		display: flex;
		align-items: center;
		justify-content: center;
		background: #fcfcfc;
		color: #2d2d2d;
		-webkit-font-smoothing: antialiased;
		-moz-osx-font-smoothing: grayscale;
	}
	.card {
		max-width: 420px;
		width: 100%;
		padding: 48px 36px;
		text-align: center;
		background: #ffffff;
		border: 1px solid #dcdcdc;
		border-radius: 12px;
		margin: 16px;
		box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1), 0 2px 4px -2px rgba(0,0,0,0.1);
		overflow: hidden;
	}
	.logo {
		display: flex;
		align-items: center;
		justify-content: center;
		gap: 10px;
		margin-bottom: 28px;
	}
	.logo-icon {
		width: 40px;
		height: 40px;
		background: #333340;
		border: 1px solid rgba(168, 85, 247, 0.3);
		border-radius: 8px;
		display: flex;
		align-items: center;
		justify-content: center;
	}
	.logo-icon svg {
		width: 24px;
		height: 24px;
	}
	.logo-text {
		font-size: 20px;
		font-weight: 700;
		color: #2d2d2d;
		letter-spacing: -0.01em;
	}
	h1 {
		font-size: 20px;
		font-weight: 600;
		margin: 0 0 8px 0;
		color: #2d2d2d;
	}
	p {
		margin: 0 0 16px 0;
		color: #666666;
		font-size: 15px;
	}
	.secondary {
		color: #8f8f8f;
		font-size: 13px;
	}
	.detail {
		background: #f8f8f8;
		border: 1px solid #e1e1e1;
		border-radius: 8px;
		padding: 12px 16px;
		font-size: 13px;
		color: #666666;
		font-family: "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
		word-break: break-word;
		margin: 12px 0 16px 0;
		text-align: left;
		line-height: 1.5;
	}
	.icon {
		width: 48px;
		height: 48px;
		margin: 0 auto 20px auto;
		border-radius: 50%;
		display: flex;
		align-items: center;
		justify-content: center;
	}
	.icon svg {
		width: 24px;
		height: 24px;
	}
	.icon-success {
		background: #dcfce7;
		color: #16a34a;
	}
	.icon-error {
		background: #fee2e2;
		color: #dc2626;
	}
	.integration {
		font-weight: 600;
		color: #2d2d2d;
	}
	button {
		background: #2c2c33;
		color: #ffffff;
		border: none;
		padding: 12px 24px;
		border-radius: 8px;
		font-size: 14px;
		font-weight: 500;
		cursor: pointer;
		margin-top: 8px;
		transition: background 150ms ease;
		width: 100%;
	}
	button:hover { background: #3d3d47; }
`

const errorHTMLTmpl = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>{{.Title}} - ` + pageTitle + `</title>
	<style>` + baseStyles + `</style>
</head>
<body>
	<script>` + closeScript + `</script>
	<div class="card">
		<div class="logo">
			<div class="logo-icon"><img src="{{LOGO_URI}}" alt="" width="24" height="24"></div>
			<span class="logo-text">` + pageTitle + `</span>
		</div>
		<div class="icon icon-error">
			<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
				<path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"/>
			</svg>
		</div>
		<h1>{{.Title}}</h1>
		<div class="detail">{{.Message}}</div>
		<p class="secondary">You can close this window.</p>
		<button id="close-btn" onclick="tryClose()">Close Window</button>
	</div>
</body>
</html>`

const successHTMLTmpl = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>{{.Title}} - ` + pageTitle + `</title>
	<style>` + baseStyles + `</style>
</head>
<body>
	<script>` + closeScript + `</script>
	<div class="card">
		<div class="logo">
			<div class="logo-icon"><img src="{{LOGO_URI}}" alt="" width="24" height="24"></div>
			<span class="logo-text">` + pageTitle + `</span>
		</div>
		<div class="icon icon-success">
			<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
				<path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/>
			</svg>
		</div>
		<h1>{{.Title}}</h1>
		<p><span class="integration">{{.IntegrationType}}</span> has been connected to your workspace.</p>
		<p class="secondary">You can close this window.</p>
		<button id="close-btn" onclick="tryClose()">Close Window</button>
	</div>
</body>
</html>`
