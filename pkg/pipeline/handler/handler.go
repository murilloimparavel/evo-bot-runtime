package handler

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
	pipelineService "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/service"
)

type Handler struct {
	repo   repository.PipelineRepository
	svc    pipelineService.PipelineService
	secret string
}

func NewHandler(repo repository.PipelineRepository, svc pipelineService.PipelineService, secret string) *Handler {
	return &Handler{repo: repo, svc: svc, secret: secret}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.POST("/events", SecretMiddleware(h.secret), h.handleEvent)
	r.GET("/health", h.handleHealth)
}

func (h *Handler) handleHealth(c *gin.Context) {
	if err := h.repo.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "error",
			"detail": "redis unreachable",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) handleEvent(c *gin.Context) {
	var event model.MessageEvent
	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid payload",
			"code":  "ERR_INVALID_EVENT",
		})
		return
	}
	if err := event.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
			"code":  "ERR_INVALID_EVENT",
		})
		return
	}

	// Process commits the complete event atomically before returning. A 5xx
	// permits the CRM outbox to retry the original message ID safely.
	if err := h.svc.Process(c.Request.Context(), &event); err != nil {
		slog.Error("pipeline.event.persist_failed", "contact_id", event.ContactID, "conversation_id", event.ConversationID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error", "code": "ERR_INTERNAL"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "accepted"})
}
