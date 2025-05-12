package auth

import (
	"net/http"
	"os"
	"strings"
)

type TenantResolver func(token string) (tenantID string, ok bool)

// Static resolver loads `TENANT_TOKENS=tenantA:tokenA,tenantB:tokenB` from env.
func StaticResolverFromEnv() TenantResolver {
	raw := os.Getenv("TENANT_TOKENS")
	pairs := map[string]string{}
	if raw == "" {
		// dev fallback
		pairs["dev-token"] = "dev-tenant"
	} else {
		for _, p := range strings.Split(raw, ",") {
			kv := strings.SplitN(p, ":", 2)
			if len(kv) == 2 {
				pairs[kv[1]] = kv[0]
			}
		}
	}
	return func(token string) (string, bool) {
		t, ok := pairs[token]
		return t, ok
	}
}

type ctxKey struct{}

var TenantCtxKey = ctxKey{}

func Middleware(resolve TenantResolver) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			const pfx = "Bearer "
			if !strings.HasPrefix(h, pfx) {
				http.Error(w, "missing bearer", http.StatusUnauthorized)
				return
			}
			tenant, ok := resolve(strings.TrimPrefix(h, pfx))
			if !ok {
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}
			ctx := r.Context()
			ctx = withTenant(ctx, tenant)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
