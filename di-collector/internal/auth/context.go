package auth

import "context"

func withTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, TenantCtxKey, tenant)
}

func TenantFrom(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(TenantCtxKey).(string)
	return v, ok && v != ""
}
