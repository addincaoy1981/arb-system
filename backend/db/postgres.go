package db

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/lib/pq"
)

var DB *sql.DB

// 初始化 PostgreSQL 连接
func InitPostgres(dsn string) {
	var err error
	DB, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("Postgres connection failed:", err)
	}

	// 高并发连接配置
	DB.SetMaxOpenConns(50)
	DB.SetMaxIdleConns(20)
	DB.SetConnMaxLifetime(time.Hour)

	if err := DB.Ping(); err != nil {
		log.Fatal("Postgres ping failed:", err)
	}

	log.Println("Postgres connected successfully")

	// 建表
	createTables()
}

// 建表语句
func createTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS tokens (
			symbol VARCHAR(20) PRIMARY KEY,
			address VARCHAR(42) NOT NULL,
			decimals INT NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		);`,
		`CREATE INDEX IF NOT EXISTS idx_tokens_address ON tokens(address);`,

		`CREATE TABLE IF NOT EXISTS pools (
			id SERIAL PRIMARY KEY,
			pair_address VARCHAR(42) NOT NULL,
			token0 VARCHAR(42) NOT NULL,
			token1 VARCHAR(42) NOT NULL,
			fee NUMERIC(5,2) DEFAULT 0,
			reserve0 NUMERIC(38,18) DEFAULT 0,
			reserve1 NUMERIC(38,18) DEFAULT 0,
			last_updated TIMESTAMP DEFAULT NOW()
		);`,
		`CREATE INDEX IF NOT EXISTS idx_pools_pair_address ON pools(pair_address);`,
		`CREATE INDEX IF NOT EXISTS idx_pools_token0_token1 ON pools(token0, token1);`,

		`CREATE TABLE IF NOT EXISTS arbitrage_trades (
			id SERIAL PRIMARY KEY,
			path TEXT[],
			expected_profit NUMERIC(38,18),
			executed BOOLEAN DEFAULT FALSE,
			success BOOLEAN,
			tx_hash VARCHAR(66),
			created_at TIMESTAMP DEFAULT NOW(),
			executed_at TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_arbitrage_trades_executed ON arbitrage_trades(executed);`,
		`CREATE INDEX IF NOT EXISTS idx_arbitrage_trades_path ON arbitrage_trades USING GIN(path);`,
	}

	for _, q := range queries {
		if _, err := DB.Exec(q); err != nil {
			log.Fatal("Failed to execute query:", q, err)
		}
	}
	log.Println("Tables and indexes ensured")
}

// 保存套利机会
func SaveArbitrage(path []string, expectedProfit float64) error {
	query := `INSERT INTO arbitrage_trades(path, expected_profit) VALUES($1,$2)`
	_, err := DB.Exec(query, path, expectedProfit)
	return err
}

// 更新套利交易执行状态
func UpdateArbitrageExecution(txHash string, success bool) error {
	query := `UPDATE arbitrage_trades SET executed=true, success=$1, tx_hash=$2, executed_at=$3 WHERE tx_hash IS NULL ORDER BY id ASC LIMIT 1`
	_, err := DB.Exec(query, success, txHash, time.Now())
	return err
}

// 保存或更新池子信息
func UpsertPool(pairAddress, token0, token1 string, fee float64, reserve0, reserve1 float64) error {
	query := `
	INSERT INTO pools(pair_address, token0, token1, fee, reserve0, reserve1, last_updated)
	VALUES ($1,$2,$3,$4,$5,$6,NOW())
	ON CONFLICT (pair_address)
	DO UPDATE SET reserve0=$5, reserve1=$6, last_updated=NOW()
	`
	_, err := DB.Exec(query, pairAddress, token0, token1, fee, reserve0, reserve1)
	return err
}

// 查询所有池子（用于内存图构建）
type Pool struct {
	PairAddress string
	Token0      string
	Token1      string
	Fee         float64
	Reserve0    float64
	Reserve1    float64
}

func GetAllPools() ([]Pool, error) {
	rows, err := DB.Query("SELECT pair_address, token0, token1, fee, reserve0, reserve1 FROM pools")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pools []Pool
	for rows.Next() {
		var p Pool
		if err := rows.Scan(&p.PairAddress, &p.Token0, &p.Token1, &p.Fee, &p.Reserve0, &p.Reserve1); err != nil {
			return nil, err
		}
		pools = append(pools, p)
	}
	return pools, nil
}
