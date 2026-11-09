package whatsapp

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/mdp/qrterminal/v3"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types/events"
)

const syncCompletionSettleDelay = 5 * time.Second

// registerHandlers registers event handlers for WhatsApp events.
func (c *Client) registerHandlers() {
	c.WA.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			c.handleMessage(v)
		case *events.HistorySync:
			result := c.handleHistorySync(v)
			if v.Data != nil && v.Data.GetSyncType() == waHistorySync.HistorySync_ON_DEMAND {
				c.completeBackfillRequest(result)
			}
			// Check if sync is complete (progress == 100)
			if v.Data != nil && v.Data.Progress != nil && *v.Data.Progress >= 100 {
				c.Logger.Info("history sync complete")
				c.backfillChatNames()
				c.signalSyncCompleteAfterSettleDelay()
			}
		case *events.OfflineSyncCompleted:
			c.backfillChatNames()
			count, _ := c.Store.CountMessages()
			if count > 0 {
				c.signalSyncCompleteAfterSettleDelay()
			}
		case *events.Connected:
			c.Logger.Info("connected to WhatsApp")
		case *events.LoggedOut:
			c.Logger.Warn("logged out of WhatsApp")
		}
	})
}

func (c *Client) signalSyncCompleteAfterSettleDelay() {
	c.syncCompleteMu.Lock()
	defer c.syncCompleteMu.Unlock()

	if c.syncCompleteTimer != nil {
		c.syncCompleteTimer.Stop()
	}

	c.syncCompleteTimer = time.AfterFunc(syncCompletionSettleDelay, func() {
		if !c.WA.IsConnected() {
			return
		}

		select {
		case c.SyncComplete <- struct{}{}:
		default:
		}
	})
}

// ConnectWithQR connects to WhatsApp, displaying a QR code if needed.
func (c *Client) ConnectWithQR(ctx context.Context) error {
	if c.WA.Store.ID == nil {
		qrChan, err := c.WA.GetQRChannel(ctx)
		if err != nil {
			return fmt.Errorf("failed to initialize QR pairing: %w", err)
		}
		if err := c.WA.Connect(); err != nil {
			return err
		}

		for {
			evt, ok := <-qrChan
			if !ok {
				if err := ctx.Err(); err != nil {
					return err
				}
				return fmt.Errorf("QR pairing ended before WhatsApp reported success")
			}

			if evt.Event == "code" {
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stderr)
				continue
			}

			switch evt.Event {
			case "success":
				return nil
			case "error":
				return fmt.Errorf("WhatsApp pairing failed: %v", evt.Error)
			case "err-client-outdated":
				return fmt.Errorf("WhatsApp rejected this client version (405); update whatsmeow and rebuild")
			case "timeout":
				return fmt.Errorf("WhatsApp QR pairing timed out")
			default:
				return fmt.Errorf("WhatsApp QR pairing ended with event %q", evt.Event)
			}
		}
	}

	return c.WA.Connect()
}

// Connect connects to WhatsApp without QR (requires existing session).
func (c *Client) Connect() error {
	return c.WA.Connect()
}
