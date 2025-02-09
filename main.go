package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *gorm.DB
var redisClient *redis.Client
var ctx = context.Background()

// var taskQueue *asynq.Client

// Loan model
type Loan struct {
	ID               uint      `gorm:"primaryKey"`
	BorrowerID       uint      `gorm:"not null"`
	Amount           float64   `gorm:"not null"`
	InterestRate     float64   `gorm:"not null"`
	WeeklyPayment    float64   `gorm:"not null"`
	RemainingBalance float64   `gorm:"not null"`
	CreatedAt        time.Time `gorm:"autoCreateTime"`
}

// Repayment model
type Repayment struct {
	ID        uint      `gorm:"primaryKey"`
	LoanID    uint      `gorm:"not null"`
	WeekNo    int       `gorm:"not null"`
	Paid      bool      `gorm:"default:false"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func init() {
	var err error
	dsn := "host=localhost user=postgres password=postgres dbname=loansystem port=5432 sslmode=disable"
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database: ", err)
	}
	db.AutoMigrate(&Loan{}, &Repayment{})

	// Initialize Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

}

func createLoan(c *gin.Context) {
	var loan Loan
	if err := c.ShouldBindJSON(&loan); err != nil {
		c.JSON(400, gin.H{"error": "Invalid input"})
		return
	}

	loan.InterestRate = 0.10
	loan.WeeklyPayment = (loan.Amount * (1 + loan.InterestRate)) / 50
	loan.RemainingBalance = loan.Amount * (1 + loan.InterestRate)
	db.Create(&loan)

	// Create repayment schedule
	for i := 1; i <= 50; i++ {
		db.Create(&Repayment{LoanID: loan.ID, WeekNo: i, Paid: false})
	}

	c.JSON(201, loan)
}

// Get outstanding balance with Redis caching
func getOutstanding(c *gin.Context) {
	id := c.Param("loan_id")
	cacheKey := fmt.Sprintf("loan:%s:outstanding", id)

	cachedBalance, err := redisClient.Get(ctx, cacheKey).Result()
	if err == nil {
		c.JSON(200, gin.H{"remaining_balance": cachedBalance})
		return
	}

	var loan Loan
	db.First(&loan, id)
	if loan.ID == 0 {
		c.JSON(404, gin.H{"error": "Loan not found"})
		return
	}

	redisClient.Set(ctx, cacheKey, fmt.Sprintf("%f", loan.RemainingBalance), 10*time.Minute)
	c.JSON(200, gin.H{"remaining_balance": loan.RemainingBalance})
}

// Make a payment with cache update using worker queue
func makePayment(c *gin.Context) {
	var loan Loan
	id := c.Param("loan_id")

	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Raw("SELECT * FROM loans WHERE id = ? FOR UPDATE", id).Scan(&loan).Error; err != nil {
			c.JSON(404, gin.H{"error": "Loan not found"})
			return err
		}

		var repayment Repayment
		if err := tx.Where("loan_id = ? AND paid = ?", loan.ID, false).
			Order("week_no asc").First(&repayment).Error; err != nil {
			c.JSON(400, gin.H{"error": "No pending repayments"})
			return err
		}

		repayment.Paid = true
		if err := tx.Save(&repayment).Error; err != nil {
			return err
		}

		loan.RemainingBalance -= loan.WeeklyPayment
		if err := tx.Save(&loan).Error; err != nil {
			return err
		}

		redisClient.Set(ctx, fmt.Sprintf("loan:%s:outstanding", id), fmt.Sprintf("%f", loan.RemainingBalance), 10*time.Minute)
		redisClient.Del(ctx, fmt.Sprintf("loan:%s:delinquent", id))

		return nil
	})

	if err == nil {
		c.JSON(200, gin.H{
			"message":           "Payment successful",
			"remaining_balance": loan.RemainingBalance,
		})
		return
	}

	c.JSON(500, gin.H{"error": "Transaction failed after multiple retries"})
}

// Check delinquency with Redis caching
func isDelinquent(c *gin.Context) {
	var loan Loan
	id := c.Param("loan_id")
	cacheKey := fmt.Sprintf("loan:%s:delinquent", id)

	cachedDelinquency, err := redisClient.Get(ctx, cacheKey).Result()
	if err == nil {
		isDelinquent, _ := strconv.ParseBool(cachedDelinquency)
		c.JSON(200, gin.H{"is_delinquent": isDelinquent})
		return
	}

	db.First(&loan, id)
	if loan.ID == 0 {
		c.JSON(404, gin.H{"error": "Loan not found"})
		return
	}

	var count int64
	db.Model(&Repayment{}).Where("loan_id = ? AND paid = ?", loan.ID, false).
		Order("week_no desc").Limit(2).Count(&count)

	isDelinquent := count >= 2
	redisClient.Set(ctx, cacheKey, strconv.FormatBool(isDelinquent), 10*time.Minute)
	c.JSON(200, gin.H{"is_delinquent": isDelinquent})
}

func main() {
	r := gin.Default()
	r.POST("/loans", createLoan)
	r.POST("/loans/:loan_id/payments", makePayment)
	r.GET("/loans/:loan_id/outstanding", getOutstanding)
	r.GET("/loans/:loan_id/delinquent", isDelinquent)

	fmt.Println("Server running on port 8080")
	r.Run(":8080")
}
