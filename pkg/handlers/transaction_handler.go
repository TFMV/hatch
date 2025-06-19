// Package handlers contains Flight SQL protocol handlers.
package handlers

import (
	"context"
	"fmt"

	"github.com/TFMV/porter/pkg/models"
	"github.com/TFMV/porter/pkg/services"
)

// transactionHandler implements TransactionHandler interface.
type transactionHandler struct {
	transactionService services.TransactionService
	logger             Logger
	metrics            MetricsCollector
}

// NewTransactionHandler creates a new transaction handler.
func NewTransactionHandler(
	transactionService services.TransactionService,
	logger Logger,
	metrics MetricsCollector,
) TransactionHandler {
	return &transactionHandler{
		transactionService: transactionService,
		logger:             logger,
		metrics:            metrics,
	}
}

// Begin starts a new transaction.
func (h *transactionHandler) Begin(ctx context.Context, readOnly bool) (string, error) {
	timer := h.metrics.StartTimer("handler_transaction_begin")
	defer timer.Stop()

	h.logger.Debug("Beginning transaction", "read_only", readOnly)

	// Create transaction options
	opts := models.TransactionOptions{
		ReadOnly: readOnly,
		// Isolation level can be set from context or defaults
	}

	// Begin transaction
	txnID, err := h.transactionService.Begin(ctx, opts)
	if err != nil {
		h.metrics.IncrementCounter("handler_transaction_begin_errors")
		h.logger.Error("Failed to begin transaction", "error", err)
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}

	h.logger.Info("Transaction started", "transaction_id", txnID)
	h.metrics.IncrementCounter("handler_transactions_started")

	return txnID, nil
}

// Commit commits a transaction.
func (h *transactionHandler) Commit(ctx context.Context, transactionID string) error {
	timer := h.metrics.StartTimer("handler_transaction_commit")
	defer timer.Stop()

	h.logger.Debug("Committing transaction", "transaction_id", transactionID)

	if transactionID == "" {
		h.metrics.IncrementCounter("handler_transaction_invalid_id")
		return fmt.Errorf("invalid transaction ID")
	}

	// Commit transaction
	if err := h.transactionService.Commit(ctx, transactionID); err != nil {
		h.metrics.IncrementCounter("handler_transaction_commit_errors")
		h.logger.Error("Failed to commit transaction", "error", err, "transaction_id", transactionID)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("Transaction committed", "transaction_id", transactionID)
	h.metrics.IncrementCounter("handler_transactions_committed")

	return nil
}

// Rollback rolls back a transaction.
func (h *transactionHandler) Rollback(ctx context.Context, transactionID string) error {
	timer := h.metrics.StartTimer("handler_transaction_rollback")
	defer timer.Stop()

	h.logger.Debug("Rolling back transaction", "transaction_id", transactionID)

	if transactionID == "" {
		h.metrics.IncrementCounter("handler_transaction_invalid_id")
		return fmt.Errorf("invalid transaction ID")
	}

	// Rollback transaction
	if err := h.transactionService.Rollback(ctx, transactionID); err != nil {
		h.metrics.IncrementCounter("handler_transaction_rollback_errors")
		h.logger.Error("Failed to rollback transaction", "error", err, "transaction_id", transactionID)
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	h.logger.Info("Transaction rolled back", "transaction_id", transactionID)
	h.metrics.IncrementCounter("handler_transactions_rolled_back")

	return nil
}
