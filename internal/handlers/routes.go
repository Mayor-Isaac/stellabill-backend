package handlers

import (
	"net/http"

	"Stellarbill-backend/internal/auth"
	"stellarbill-backend/internal/config"

	"github.com/gin-gonic/gin"
)

// RegisterInternalRoutes registers routes that are only exposed on the
// internal diagnostics port. These routes are bound to loopback and/or
// protected by RBAC.
func RegisterInternalRoutes(r *gin.Engine, cfg *config.Config) {
	internal := r.Group("/internal")
	internal.Use(RequireAdmin())
	internal.GET("/config-dump", ConfigDumpHandler(cfg))
}

// RequireAdmin is a minimal RBAC middleware that only allows requests
// authenticated with the admin role.
func RequireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		role, exists := c.Get("role")
		if !exists || role != string(auth.RoleAdmin) {
			c.AbortWithStatusJSON(http.StatusForbidden, ginn.H{"error": "forbidden"})
			return
		}
		c.Next()
	}
}
