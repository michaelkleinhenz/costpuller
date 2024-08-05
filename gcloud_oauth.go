package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// defaultTokenCachePath is the path, relative to the platform's user cache
// directory, to the directory where cache files are stored.
const defaultTokenCachePath = "gcloud"

// tokenFileName is the name of the file which is used to store the cached
// OAuth 2.0 access and refresh token values.
const tokenFileName = "costpuller_token.json"

// getGoogleOAuthHttpClient accepts a mapping of configuration value strings
// and returns an HTTP client which can be used to make authorized Google API
// requests.  The token is obtained either using values cached in a local file
// or by prompting the user to perform an authorization dialog; either way, the
// new token is written to the cache file before returning.
//
// The Google OAuth 2.0 Client configuration is constructed from a local
// credentials file (which can be downloaded from https://console.developers.google.com,
// under "Credentials").  It is located using the default mechanisms (e.g., in
// ${HOME}/.config/gcloud/application_default_credentials.json).  (Currently,
// the scope of the authorization is limited to the Google Sheets APIs.)
func getGoogleOAuthHttpClient(oauthConfigMap map[string]string) *http.Client {
	ctx := context.Background()

	credObj, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		log.Fatalf("Unable to read OAuth client credentials file: %v", err)
	}

	config, err := google.ConfigFromJSON(credObj.JSON, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		log.Fatalf("Unable to construct a client configuration: %v", err)
	}

	token, tokenCachePath := getToken(oauthConfigMap, config, ctx)
	cacheToken(token, tokenCachePath)

	return config.Client(ctx, token)
}

// getToken is a helper function which extracts configuration information from
// the supplied mapping and returns either a cached token, if available, or a
// new token.
func getToken(
	oauthConfigMap map[string]string,
	config *oauth2.Config,
	ctx context.Context,
) (token *oauth2.Token, tokenCachePath string) {
	var tokenCacheFile *os.File
	tokenCachePath, err := getCacheFileName(oauthConfigMap["tokenCachePath"])
	if err == nil {
		tokenCacheFile, err = os.Open(tokenCachePath)
	}
	if err == nil {
		token = getCachedToken(config, tokenCacheFile, ctx)
		closeFile(tokenCacheFile)
	} else if errors.Is(err, os.ErrNotExist) {
		token = getNewToken(config, oauthConfigMap["port"], ctx)
	} else {
		log.Fatalf("Unexpected error accessing the token cache file, %q: %v", tokenCachePath, err)
	}
	return
}

// cacheToken is a helper function which accepts a token and a file path and
// stores the token in the indicated file.  The contents of the file are
// replaced with the new value.  If the path is blank, the function prints a
// message and returns; other errors result in exiting the process.
func cacheToken(token *oauth2.Token, tokenCachePath string) {
	if tokenCachePath == "" {
		log.Println("The token will not be cached.")
	} else {
		newTokenCacheFile, err := os.OpenFile(tokenCachePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err == nil {
			log.Printf("Caching oauth token in %q.", tokenCachePath)
			err = json.NewEncoder(newTokenCacheFile).Encode(token)
			closeFile(newTokenCacheFile)
		}
		if err != nil {
			log.Printf("Unable to cache oauth token: %v", err)
		}
	}
}

// getCacheFileName accepts a file path to the directory containing the token
// cache file and returns an absolute path to the cached token file or an
// error.  If the input path is an empty string, the default path is used; if
// the path is relative, it is prefixed with the platform's user configuration
// directory.  The token file name is appended to the path and the result is
// returned.
func getCacheFileName(tokenCachePath string) (string, error) {
	if tokenCachePath == "" {
		tokenCachePath = defaultTokenCachePath
	}
	if tokenCachePath[0] != '/' {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			log.Printf("unable to determine cache directory: %v", err)
			return "", fmt.Errorf("%w", os.ErrNotExist)
		}
		tokenCachePath = filepath.Join(cacheDir, tokenCachePath)
		if err := os.MkdirAll(tokenCachePath, 0700); err != nil {
			log.Printf("unable to create user cache dir, %q: %v", cacheDir, err)
			return "", fmt.Errorf("%w", os.ErrNotExist)
		}
	}
	return filepath.Join(tokenCachePath, tokenFileName), nil
}

// getCachedToken is a helper function which reads a cached token from the
// provided file, refreshes it using the provided configuration and context,
// and returns the resulting token.
func getCachedToken(config *oauth2.Config, cacheFile *os.File, ctx context.Context) *oauth2.Token {
	token := &oauth2.Token{}
	err := json.NewDecoder(cacheFile).Decode(token)
	if err != nil {
		log.Fatalf("Unable to parse cached OAuth tokens, %q: %v", cacheFile.Name(), err)
	}

	token, err = config.TokenSource(ctx, token).Token()
	if err != nil {
		log.Fatalf("Unable to refresh the cached OAuth tokens: %v", err)
	}

	return token
}

// getNewToken is a helper function which prompts the user to use their browser
// to request a new token, obtains the access code when the request is
// redirected to the local listener, exchanges the access code for an access
// token and a refresh token, and returns the token-pair.  The supplied
// configuration is used to access the OAuth 2.0 client configuration to
// generate the access request URL; the redirect URL is modified to include
// a custom port (otherwise, it would default to port 80, which is not
// generally available); and, a random number ("state") is included in the
// request and checked in the redirect to prevent man-in-the-middle attacks.
// After prompting the user, a local listener for the redirect request is
// started, and execution waits for the redirected request which includes the
// access code in the request query parameters.
func getNewToken(config *oauth2.Config, listenerPort string, ctx context.Context) *oauth2.Token {
	stateToken := getStateToken()
	if listenerPort == "" {
		listenerPort = "35355" // Arbitrary value
	}
	config.RedirectURL += ":" + listenerPort
	authURL := config.AuthCodeURL(stateToken, oauth2.AccessTypeOffline)
	fmt.Printf("\nGo to the following link in your browser to authorize access:\n%v\n\n", authURL)

	// Listen for the redirect request, then extract the authorization code
	// from the resulting query params.
	queryParams := redirectListener(config.RedirectURL)
	authCode := getAuthCode(queryParams, stateToken)

	// Exchange the authorization code for an access token and refresh token.
	token, err := config.Exchange(ctx, authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve access token: %v", err)
	}
	return token
}

// getStateToken creates a random state token which is used to validate the
// OAuth redirect request.  The token is the base64-encoded SHA256 hash of the
// current time as a string.
func getStateToken() string {
	h := sha256.New()
	h.Write([]byte(time.Now().Format("20060102150405000000")))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// getAuthCode validates the result of the redirect from the user's
// authorization request, and returns the access code if one is received;
// otherwise it exits the process with a failure.
func getAuthCode(authResp url.Values, stateToken string) string {
	if authResp.Get("state") != stateToken {
		log.Fatalf(
			"Error in authorization state, expected %q, got %q",
			stateToken,
			authResp.Get("state"),
		)
	}
	if authResp.Get("error") != "" {
		log.Fatalf("Error returned from authorization: %s", authResp.Get("error"))
	}
	authCode := authResp.Get("code")
	if authCode == "" {
		log.Fatalf("No authorization code received.")
	}
	return authCode
}

// redirectListener is a helper function used in the creation of the Google API
// client.  It sets up a micro-webserver which listens for a single request to
// the provided URL.  Errors parsing the redirect URL input or starting the
// micro-webserver are logged with Fatalf() which exits the process.
//
// When the request is received, the request is acknowledged, the webserver is
// shut down, and the query parameters of the request (presumably the state
// token and the access code; or an error) are returned.  The request (in the
// user's browser) looks something like this:
//
//	http://localhost/?state=<state_token>&code=<auth_code>&scope=<auth_scopes>
func redirectListener(urlString string) url.Values {
	// This variable is set by the request handler (it is included in the
	// function's closure) and returned after the micro-webserver exits.
	var queryParams url.Values

	// Configure the micro-webserver, add a handler to it for the default
	// route, and start the listener which will serve requests until the
	// server is shut down.
	mux := http.NewServeMux()
	server := http.Server{Addr: getListenAddress(urlString), Handler: mux}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		queryParams = r.URL.Query()
		handleRedirectResponse(w, queryParams)
		// Request the server shutdown in a separate goroutine to allow it to
		// wait for this request to finish processing.
		go requestShutdown(&server)
	})

	// Run the webserver, listening for and dispatching requests, until
	// shutdown is requested.
	if err := server.ListenAndServe(); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Error running redirect listener: %v", err)
		}
	}

	return queryParams
}

// handleRedirectResponse is a helper function which evaluates the redirect
// query parameters and sends an appropriate response to the request.
func handleRedirectResponse(w http.ResponseWriter, queryParams url.Values) {
	msg := `<!doctype html><html lang="en" dir="ltr"><body><h2>`
	if queryParams.Get("code") == "" {
		msg += "Failure -- no access code received!</h2>"
		if queryParams.Get("error") != "" {
			msg += "<h3>Error:  " + queryParams.Get("error") + "</h3>"
		}
	} else {
		msg += "Success!  Access code received.</h2>"
	}
	msg += "<p>You may close this browser window.</body></html>"
	_, err := fmt.Fprint(w, msg)
	if err != nil {
		log.Printf("Error writing response to redirect request: %v", err)
	}
}

// requestShutdown is a helper function which requests the server to shut down,
// packaged as a separate function to make it easy to run as a goroutine.
func requestShutdown(server *http.Server) {
	err := server.Shutdown(context.Background())
	if err != nil {
		log.Fatalf("Error shutting down redirect listener: %v", err)
	}
}

// RedirectUrlPattern matches a host (e.g., "localhost" or a FQDN) with an
// optional "http" schema and an optional port.  This is the location provided
// in the OAuth 2.0 client configuration where the authorization flow redirects
// the request after it has been granted or denied.  The schema, if any is
// ignored; path specifications are not supported -- only host (and optionally
// port) should be provided.  The host must resolve to a NIC on the machine
// where this program is being run.
var RedirectUrlPattern = regexp.MustCompile(`^(?:http://)?([^:/]+)(:[0-9]{1,5})$`)

// getListenAddress validates the redirect URL, strips the schema if present,
// sets the address to the host, appends the port if present, and returns the
// result.
func getListenAddress(urlString string) string {
	matches := RedirectUrlPattern.FindStringSubmatch(urlString)
	if matches == nil {
		log.Fatalf("Could not parse redirect URL: %s", urlString)
	}
	address := matches[1]
	if matches[2] != "" {
		address += matches[2]
	}
	return address
}
