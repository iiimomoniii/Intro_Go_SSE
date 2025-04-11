package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	_ "github.com/lib/pq"
)

// Transaction represents the transaction_details table
type Transaction struct {
	TaNo           string     `json:"ta_no"`
	RefNo          string     `json:"ref_no"`
	VendorName     string     `json:"vendor_name"`
	Status         string     `json:"status"`
	CalculatedBy   string     `json:"calculated_by"`
	LastCalculated *time.Time `json:"last_calculated"`
	SettledBy      string     `json:"settled_by"`
	LastSettled    *time.Time `json:"last_settled"`
	VendorCode     string     `json:"vendor_code"`
	CreatedAt      *time.Time `json:"created_at"`
	CreatedBy      string     `json:"created_by"`
	UpdatedAt      *time.Time `json:"updated_at"`
	UpdatedBy      string     `json:"updated_by"`
}

// Config represents database connection config
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

var db *sql.DB
var clients = make(map[*fiber.Ctx]chan string)

func main() {
	// Database configuration
	config := Config{
		
	}

	// Initialize database connection
	connectDB(config)
	defer db.Close()

	app := fiber.New()

	// CORS middleware
	app.Use(cors.New(cors.Config{
		AllowOrigins:     "http://localhost:3000",
		AllowHeaders:     "Origin, Content-Type, Accept",
		AllowCredentials: true,
	}))

	// API routes
	app.Get("/api/transactions", getTransactions)
	app.Put("/api/transactions/:taNo/calculate", updateTransactionStatus)
	app.Get("/api/sse", handleSSE)

	log.Fatal(app.Listen(":8080"))
}

func connectDB(config Config) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.DBName, config.SSLMode,
	)

	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("Connected to database successfully")
}
func getTransactions(c *fiber.Ctx) error {
	// First, add more detailed error logging
	rows, err := db.Query(`
        SELECT 
            ta_no, ref_no, vendor_name, status, calculated_by, 
            last_calculated, settled_by, last_settled, vendor_code,
            created_at, created_by, updated_at, updated_by
        FROM 
            transaction_details
    `)
	if err != nil {
		log.Printf("Database query error: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Error fetching transactions",
		})
	}
	defer rows.Close()

	var transactions []Transaction
	for rows.Next() {
		var t Transaction

		// Use nullable types for fields that might be NULL
		var refNo, vendorName, status, calculatedBy, settledBy, vendorCode, createdBy, updatedBy sql.NullString
		var lastCalculated, lastSettled, createdAt, updatedAt sql.NullTime

		// Scan into nullable types first
		err := rows.Scan(
			&t.TaNo, &refNo, &vendorName, &status, &calculatedBy,
			&lastCalculated, &settledBy, &lastSettled, &vendorCode,
			&createdAt, &createdBy, &updatedAt, &updatedBy,
		)

		if err != nil {
			log.Printf("Error scanning row: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Error scanning transactions",
			})
		}

		// Convert nullable types to struct fields
		t.RefNo = refNo.String
		t.VendorName = vendorName.String
		t.Status = status.String
		t.CalculatedBy = calculatedBy.String
		t.SettledBy = settledBy.String
		t.VendorCode = vendorCode.String
		t.CreatedBy = createdBy.String
		t.UpdatedBy = updatedBy.String

		if lastCalculated.Valid {
			t.LastCalculated = &lastCalculated.Time
		}
		if lastSettled.Valid {
			t.LastSettled = &lastSettled.Time
		}
		if createdAt.Valid {
			t.CreatedAt = &createdAt.Time
		}
		if updatedAt.Valid {
			t.UpdatedAt = &updatedAt.Time
		}

		transactions = append(transactions, t)
	}

	return c.JSON(transactions)
}

func updateTransactionStatus(c *fiber.Ctx) error {
	taNo := c.Params("taNo")
	username := c.Query("username", "system") // Get username from query or default to "system"

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to begin transaction",
		})
	}

	// First update status to "Calculating"
	_, err = tx.Exec(`
		UPDATE transaction_details 
		SET status = 'Calculating', 
		    updated_at = NOW(),
		    updated_by = $1
		WHERE ta_no = $2
	`, username, taNo)

	if err != nil {
		tx.Rollback()
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to update transaction status",
		})
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to commit transaction",
		})
	}

	// Broadcast the status change
	broadcastStatusChange(taNo, "Calculating")

	// Simulate calculation process
	go func() {
		// Simulate processing time
		time.Sleep(5 * time.Second)

		// Update to "Calculated" after processing
		db.Exec(`
			UPDATE transaction_details 
			SET status = 'Calculated', 
				calculated_by = $1,
				last_calculated = NOW(),
				updated_at = NOW(),
				updated_by = $1
			WHERE ta_no = $2
		`, username, taNo)

		// Broadcast the status change
		broadcastStatusChange(taNo, "Calculated")
	}()

	return c.JSON(fiber.Map{
		"message": "Transaction calculation started",
		"ta_no":   taNo,
	})
}

func broadcastStatusChange(taNo string, status string) {
	// Create a message to broadcast
	message := fmt.Sprintf(`{"ta_no": "%s", "status": "%s", "timestamp": "%s"}`,
		taNo, status, time.Now().Format(time.RFC3339))

	// Broadcast to all connected clients
	for _, client := range clients {
		select {
		case client <- message:
			// Message sent successfully
		default:
			// Channel is either full or closed, skip this client
		}
	}
}

func handleSSE(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("Transfer-Encoding", "chunked")

	// Create a channel for this client
	messageChan := make(chan string, 10) // Buffer for 10 messages
	clients[c] = messageChan

	// The correct signature for fasthttp.StreamWriter is func(w *bufio.Writer)
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		defer delete(clients, c)
		defer close(messageChan)

		// Send initial connection message
		w.WriteString("event: connected\ndata: Connected to SSE\n\n")
		w.Flush()

		// Keep-alive ticker to detect disconnections
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case msg, ok := <-messageChan:
				if !ok {
					return
				}
				_, err := w.WriteString(fmt.Sprintf("event: update\ndata: %s\n\n", msg))
				if err != nil {
					return // Client disconnected
				}
				w.Flush()

			case <-ticker.C:
				// Send keep-alive comment
				_, err := w.WriteString(": ping\n\n")
				if err != nil {
					return // Client disconnected
				}
				w.Flush()
			}
		}
	})

	return nil
}
