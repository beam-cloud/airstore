package apiv1

import (
	"html/template"
	"net/http"

	"github.com/labstack/echo/v4"
)

const (
	pageTitle        = "Airstore"
	pageTitleSuccess = "Connected"
	pageTitleError   = "Connection Failed"
)

var (
	errorTemplate   = template.Must(template.New("error").Parse(errorHTML))
	successTemplate = template.Must(template.New("success").Parse(successHTML))
)

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

const baseStyles = `
	*, *::before, *::after { box-sizing: border-box; }
	body {
		font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
		line-height: 1.5;
		margin: 0;
		min-height: 100vh;
		display: flex;
		align-items: center;
		justify-content: center;
		background: #fafafa;
		color: #111;
	}
	@media (prefers-color-scheme: dark) {
		body { background: #111; color: #fafafa; }
		.card { background: #1a1a1a; border-color: #333; }
		.secondary { color: #888; }
	}
	.card {
		max-width: 400px;
		padding: 48px 32px;
		text-align: center;
		background: #fff;
		border: 1px solid #e5e5e5;
		border-radius: 12px;
		margin: 16px;
	}
	h1 {
		font-size: 24px;
		font-weight: 600;
		margin: 0 0 8px 0;
	}
	p {
		margin: 0 0 16px 0;
	}
	.secondary {
		color: #666;
		font-size: 14px;
	}
	.icon {
		width: 48px;
		height: 48px;
		margin: 0 auto 24px auto;
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
	@media (prefers-color-scheme: dark) {
		.icon-success { background: #14532d; }
		.icon-error { background: #450a0a; }
	}
	.integration {
		font-weight: 600;
	}
	button {
		background: #111;
		color: #fff;
		border: none;
		padding: 10px 20px;
		border-radius: 6px;
		font-size: 14px;
		cursor: pointer;
		margin-top: 8px;
	}
	@media (prefers-color-scheme: dark) {
		button { background: #fafafa; color: #111; }
	}
`

const errorHTML = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>{{.Title}} - ` + pageTitle + `</title>
	<style>` + baseStyles + `</style>
</head>
<body>
	<div class="card">
		<div class="icon icon-error">
			<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
				<path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"/>
			</svg>
		</div>
		<h1>{{.Title}}</h1>
		<p>{{.Message}}</p>
		<p class="secondary">You can close this window and try again.</p>
		<button onclick="window.close()">Close Window</button>
	</div>
</body>
</html>`

const successHTML = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>{{.Title}} - ` + pageTitle + `</title>
	<style>` + baseStyles + `</style>
</head>
<body>
	<div class="card">
		<div class="icon icon-success">
			<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
				<path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/>
			</svg>
		</div>
		<h1>{{.Title}}</h1>
		<p><span class="integration">{{.IntegrationType}}</span> has been connected to your workspace.</p>
		<p class="secondary">You can close this window and return to the CLI.</p>
		<button onclick="window.close()">Close Window</button>
	</div>
</body>
</html>`
