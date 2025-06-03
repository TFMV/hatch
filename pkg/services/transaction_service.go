// Package services contains business logic implementations.
package services

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/TFMV/hatch/pkg/errors"
	"github.com/TFMV/hatch/pkg/models"
	"github.com/TFMV/hatch/pkg/repositories"
)

// transactionService implements TransactionService interface.
type transactionService struct {
	repo               repositories.TransactionRepository
	activeTransactions *sync.Map
	cleanupInterval    time.Duration
	logger             Logger
	metrics            MetricsCollector

	// Context-based lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTransactionService creates a new transaction service.
func NewTransactionService(
	repo repositories.TransactionRepository,
	cleanupInterval time.Duration,
	logger Logger,
	metrics MetricsCollector,
) TransactionService {
	ctx, cancel := context.WithCancel(context.Background())

	ts := &transactionService{
		repo:               repo,
		activeTransactions: &sync.Map{},
		cleanupInterval:    cleanupInterval,
		logger:             logger,
		metrics:            metrics,
		ctx:                ctx,
		cancel:             cancel,
	}

	// Start cleanup routine if interval is set
	if cleanupInterval > 0 {
		ts.wg.Add(1)
		go ts.cleanupRoutine(ctx)
	}

	return ts
}

// Begin starts a new transaction.
func (s *transactionService) Begin(ctx context.Context, opts models.TransactionOptions) (string, error) {
	timer := s.metrics.StartTimer("transaction_begin")
	defer timer.Stop()

	s.logger.Debug("Beginning transaction",
		"isolation_level", opts.IsolationLevel,
		"read_only", opts.ReadOnly,
		"timeout", opts.Timeout)

	// Validate options
	if err := s.validateTransactionOptions(opts); err != nil {
		s.metrics.IncrementCounter("transaction_validation_errors")
		return "", err
	}

	// Begin transaction in repository
	txn, err := s.repo.Begin(ctx, opts)
	if err != nil {
		s.metrics.IncrementCounter("transaction_begin_errors")
		s.logger.Error("Failed to begin transaction", "error", err)
		return "", errors.Wrap(err, errors.CodeTransactionFailed, "failed to begin transaction")
	}

	// Generate transaction ID if not provided by repository
	txnID := txn.ID()
	if txnID == "" {
		txnID = uuid.New().String()
	}

	// Store in active transactions map
	s.activeTransactions.Store(txnID, &transactionInfo{
		transaction: txn,
		startTime:   time.Now(),
		options:     opts,
	})

	// Update metrics
	s.metrics.IncrementCounter("transactions_created")
	s.updateActiveTransactionGauge()

	s.logger.Info("Transaction started", "transaction_id", txnID)

	return txnID, nil
}

// Get retrieves an existing transaction by ID.
func (s *transactionService) Get(ctx context.Context, id string) (repositories.Transaction, error) {
	timer := s.metrics.StartTimer("transaction_get")
	defer timer.Stop()

	s.logger.Debug("Getting transaction", "transaction_id", id)

	// Check active transactions first
	if info, ok := s.activeTransactions.Load(id); ok {
		txnInfo := info.(*transactionInfo)

		// Check if transaction is still valid
		if !txnInfo.transaction.IsActive() {
			s.metrics.IncrementCounter("transaction_inactive_access")
			s.activeTransactions.Delete(id)
			s.updateActiveTransactionGauge()
			return nil, errors.ErrTransactionNotFound.WithDetail("transaction_id", id)
		}

		// Check timeout
		if txnInfo.options.Timeout > 0 {
			if time.Since(txnInfo.startTime) > txnInfo.options.Timeout {
				s.metrics.IncrementCounter("transaction_timeouts")
				s.logger.Warn("Transaction timed out", "transaction_id", id)
				// Rollback timed out transaction
				_ = s.Rollback(ctx, id)
				return nil, errors.New(errors.CodeDeadlineExceeded, "transaction timed out")
			}
		}

		return txnInfo.transaction, nil
	}

	// Try to get from repository
	txn, err := s.repo.Get(ctx, id)
	if err != nil {
		if errors.IsNotFound(err) {
			s.metrics.IncrementCounter("transaction_not_found")
			return nil, errors.ErrTransactionNotFound.WithDetail("transaction_id", id)
		}
		s.metrics.IncrementCounter("transaction_get_errors")
		s.logger.Error("Failed to get transaction", "error", err, "transaction_id", id)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to get transaction")
	}

	// Add to active transactions if found
	if txn != nil && txn.IsActive() {
		s.activeTransactions.Store(id, &transactionInfo{
			transaction: txn,
			startTime:   time.Now(),
			options:     models.TransactionOptions{},
		})
		s.updateActiveTransactionGauge()
	}

	return txn, nil
}

// Commit commits a transaction.
func (s *transactionService) Commit(ctx context.Context, id string) error {
	timer := s.metrics.StartTimer("transaction_commit")
	defer timer.Stop()

	s.logger.Debug("Committing transaction", "transaction_id", id)

	// Get transaction
	txn, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	// Commit transaction
	if err := txn.Commit(ctx); err != nil {
		s.metrics.IncrementCounter("transaction_commit_errors")
		s.logger.Error("Failed to commit transaction", "error", err, "transaction_id", id)
		return errors.Wrap(err, errors.CodeTransactionFailed, "failed to commit transaction")
	}

	// Remove from active transactions
	if info, ok := s.activeTransactions.LoadAndDelete(id); ok {
		txnInfo := info.(*transactionInfo)
		duration := time.Since(txnInfo.startTime)
		s.metrics.RecordHistogram("transaction_duration", duration.Seconds())
	}

	// Remove from repository
	_ = s.repo.Remove(ctx, id)

	// Update metrics
	s.metrics.IncrementCounter("transactions_committed")
	s.updateActiveTransactionGauge()

	s.logger.Info("Transaction committed", "transaction_id", id)

	return nil
}

// Rollback rolls back a transaction.
func (s *transactionService) Rollback(ctx context.Context, id string) error {
	timer := s.metrics.StartTimer("transaction_rollback")
	defer timer.Stop()

	s.logger.Debug("Rolling back transaction", "transaction_id", id)

	// Get transaction
	txn, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	// Rollback transaction
	if err := txn.Rollback(ctx); err != nil {
		s.metrics.IncrementCounter("transaction_rollback_errors")
		s.logger.Error("Failed to rollback transaction", "error", err, "transaction_id", id)
		return errors.Wrap(err, errors.CodeTransactionFailed, "failed to rollback transaction")
	}

	// Remove from active transactions
	if info, ok := s.activeTransactions.LoadAndDelete(id); ok {
		txnInfo := info.(*transactionInfo)
		duration := time.Since(txnInfo.startTime)
		s.metrics.RecordHistogram("transaction_duration", duration.Seconds())
	}

	// Remove from repository
	_ = s.repo.Remove(ctx, id)

	// Update metrics
	s.metrics.IncrementCounter("transactions_rolled_back")
	s.updateActiveTransactionGauge()

	s.logger.Info("Transaction rolled back", "transaction_id", id)

	return nil
}

// List returns all active transactions.
func (s *transactionService) List(ctx context.Context) ([]repositories.Transaction, error) {
	timer := s.metrics.StartTimer("transaction_list")
	defer timer.Stop()

	s.logger.Debug("Listing active transactions")

	// Get from repository
	txns, err := s.repo.List(ctx)
	if err != nil {
		s.metrics.IncrementCounter("transaction_list_errors")
		s.logger.Error("Failed to list transactions", "error", err)
		return nil, errors.Wrap(err, errors.CodeInternal, "failed to list transactions")
	}

	// Filter out inactive transactions
	active := make([]repositories.Transaction, 0, len(txns))
	for _, txn := range txns {
		if txn.IsActive() {
			active = append(active, txn)
		}
	}

	s.logger.Info("Listed active transactions", "count", len(active))

	return active, nil
}

// CleanupInactive removes inactive transactions.
func (s *transactionService) CleanupInactive(ctx context.Context) error {
	timer := s.metrics.StartTimer("transaction_cleanup")
	defer timer.Stop()

	s.logger.Debug("Cleaning up inactive transactions")

	cleanupCount := atomic.Int32{}
	now := time.Now()

	// Clean up from active transactions map
	s.activeTransactions.Range(func(key, value interface{}) bool {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return false
		}

		txnID := key.(string)
		txnInfo := value.(*transactionInfo)

		// Check if transaction is inactive
		if !txnInfo.transaction.IsActive() {
			s.activeTransactions.Delete(txnID)
			cleanupCount.Add(1)
			return true
		}

		// Check if transaction has timed out
		if txnInfo.options.Timeout > 0 {
			if now.Sub(txnInfo.startTime) > txnInfo.options.Timeout {
				s.logger.Warn("Cleaning up timed out transaction", "transaction_id", txnID)

				// Create a timeout context for rollback
				rollbackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := txnInfo.transaction.Rollback(rollbackCtx); err != nil {
					s.logger.Error("Failed to rollback timed out transaction",
						"error", err,
						"transaction_id", txnID)
				}
				cancel()

				s.activeTransactions.Delete(txnID)
				_ = s.repo.Remove(ctx, txnID)
				cleanupCount.Add(1)
			}
		}

		return true
	})

	// Update metrics
	count := cleanupCount.Load()
	if count > 0 {
		s.metrics.IncrementCounter("transactions_cleaned_up", "count", fmt.Sprintf("%d", count))
		s.updateActiveTransactionGauge()
	}

	s.logger.Info("Cleaned up inactive transactions", "count", count)

	return nil
}

// cleanupRoutine runs periodic cleanup of inactive transactions until ctx is cancelled.
func (s *transactionService) cleanupRoutine(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	s.logger.Info("Transaction cleanup routine started", "interval", s.cleanupInterval)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Transaction cleanup routine stopped")
			return
		case <-ticker.C:
			// Create a per-cleanup timeout
			cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := s.CleanupInactive(cleanupCtx); err != nil {
				s.logger.Error("Cleanup routine failed", "error", err)
			}
			cancel() // Immediate cleanup
		}
	}
}

// Stop stops the transaction service gracefully.
func (s *transactionService) Stop() {
	s.logger.Info("Stopping transaction service")

	// Cancel context to signal shutdown
	s.cancel()

	// Wait for cleanup routine to finish
	s.wg.Wait()

	s.logger.Info("Transaction service stopped")
}

// validateTransactionOptions validates transaction options.
func (s *transactionService) validateTransactionOptions(opts models.TransactionOptions) error {
	// Validate isolation level
	validIsolationLevels := map[string]bool{
		"":                   true, // Empty means default
		"READ UNCOMMITTED":   true,
		"READ COMMITTED":     true,
		"REPEATABLE READ":    true,
		"SERIALIZABLE":       true,
		"SNAPSHOT":           true,
		"SNAPSHOT ISOLATION": true,
	}

	if !validIsolationLevels[string(opts.IsolationLevel)] {
		return errors.New(errors.CodeInvalidRequest, "invalid isolation level: "+string(opts.IsolationLevel))
	}

	// Validate timeout
	if opts.Timeout < 0 {
		return errors.New(errors.CodeInvalidRequest, "timeout cannot be negative")
	}

	return nil
}

// updateActiveTransactionGauge updates the active transaction count metric.
func (s *transactionService) updateActiveTransactionGauge() {
	count := 0
	s.activeTransactions.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	s.metrics.RecordGauge("active_transactions", float64(count))
}

// transactionInfo holds information about an active transaction.
type transactionInfo struct {
	transaction repositories.Transaction
	startTime   time.Time
	options     models.TransactionOptions
}
