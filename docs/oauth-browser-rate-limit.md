# OAuth Browser-Open Rate Limit

## What it does

`handleOAuthAuthorization` in `internal/upstream/core/connection.go` gates
automatic (non-manual) OAuth browser opens on a burst counter: if **5 or
more** automatic attempts for the same upstream server were recorded within
the last **1 minute**, further automatic opens are suppressed (the user is
shown the auth URL to open manually instead) until the count drops back
below the threshold.

The counter is tracked globally per-server in `internal/oauth/config.go`
(`RecordBrowserOpen` / `RecentBrowserOpenCount`), not per-`Client` instance,
since a new `Client` is typically constructed on each reconnect attempt.

## Why burst-based, not a flat cooldown

The original implementation (upstream commit `13034ebd`) blocked any
automatic open within 5 minutes of the *previous* open, regardless of
whether that previous open was itself part of a genuine retry storm. That
made it just as easy for a single automatic retry landing shortly after an
unrelated attempt to get throttled as it was to catch a real storm — the
rate limit no longer needed to be that blunt once the fork's later
concurrency fixes (state-routed callbacks, per-flow expiry timer, the
reconnect-storm fix cluster) reduced the retry volume a well-behaved client
generates. Gating on a burst count over a short window still catches a
genuine storm while letting a normal handful of spaced-out retries through.

## Why manual flows don't feed the counter

`handleOAuthAuthorizationWithResult` and `StartOAuthFlowQuick` are only ever
reached via explicit user-initiated `auth login` flows (`StartManualOAuth`,
`StartManualOAuthQuick`) and have always bypassed the rate limit itself.
They no longer call `RecordBrowserOpen` either — a manual login shouldn't
consume budget from, or be constrained by, the automatic-attempt window,
since the requirement this limiter enforces is specifically about
*automatic* reconnect storms.
