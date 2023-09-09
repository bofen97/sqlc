package SQLConn

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	arxiv "github.com/bofen97/musical-spoon"
	_ "github.com/go-sql-driver/mysql"
)

/*
1) sqlc := new(SQLConn)
2) sqlc.Connect()
3) sqlc.CreateTable()
4) sqlc.PutAllTopics()
*/
type SQLConn struct {
	db *sql.DB
}

// "root:@(127.0.0.1:3306)/arxivInfo?parseTime=true"
func (sqlc *SQLConn) Connect(url string) (err error) {

	sqlc.db, err = sql.Open("mysql", url)
	if err != nil {
		log.Fatal(err)
		return err
	}
	err = sqlc.db.Ping()
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func (sqlc *SQLConn) CreateTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS topicSummary (
		id TEXT NOT NULL,
		updated TEXT NOT NULL,
		published TEXT NOT NULL,
		title TEXT NOT NULL,
		authors TEXT NOT NULL,
		summary TEXT NOT NULL,
		topic CHAR(25) NOT NULL,
		date  DATE   NOT NULL,
		created_at DATETIME,
		hash VARCHAR(255) NOT NULL
	);`

	if _, err := sqlc.db.Exec(query); err != nil {
		log.Fatal(err)
		return err
	}
	return nil

}

// topic := "cs.ai"
func (sqlc *SQLConn) PutToTable(topic string) error {
	var v = new(arxiv.Result)
	err := v.MakeResultFromCate(topic)
	if err != nil {

		log.Fatal(err)
		return err
	}
	for _, entry := range v.Entry {

		id := entry.Id
		updated := entry.Updated
		published := entry.Published
		title := entry.Title
		summary := entry.Summary
		authors := ""
		for _, author := range entry.Authors {
			authors += author.Name + ","
		}
		created_at := time.Now()
		dateSort := strings.Split(published, "T")[0]
		hash := sha256.Sum256([]byte(summary))
		hashStr := fmt.Sprintf("%x", hash)
		flag, err := sqlc.QueryHash(hashStr)
		if err != nil {
			log.Fatal(err)
			return err
		}
		if flag {
			log.Printf("[***] Hash Hitting %s\n", hashStr)
			continue
		}
		insert_str := `
		insert into topicSummary(
			id,
			updated,
			published,
			title,
			authors,
			summary,
			topic,
			date,
			created_at,
			hash
		) values(?,?,?,?,?,?,?,?,?,?)`
		_, err = sqlc.db.Exec(insert_str, id, updated, published, title, authors, summary, topic, dateSort, created_at, hashStr)

		if err != nil {

			log.Fatal(err)
			return err
		}

	}
	return nil
}

func (sqlc *SQLConn) PutAllTopics() error {
	ch := make(chan error)

	for _, topic := range arxiv.Topics {
		for _, v := range topic.SubTopics {

			go func(code string) {

				log.Printf("Put key %s into database\n", strings.ToLower(v.Code))

				ch <- sqlc.PutToTable(strings.ToLower(code))
			}(v.Code)

			time.Sleep(3 * time.Second)

		}
	}
	for err := range ch {
		if err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil

}

// Query filed from db/topicSummary
type TiAuSuId struct {
	Title   string `json:"title"`
	Authors string `json:"authors"`
	Summary string `json:"summary"`
	Id      string `json:"url"`
}

func (sqlc *SQLConn) QueryTitleAuthorsSummaryId(topic string, date string) ([]byte, error) {
	query := "select id, title , summary,authors  from topicSummary where topic=? and date=?"
	rows, err := sqlc.db.Query(query, topic, date)

	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer rows.Close()
	var tmps []TiAuSuId
	for rows.Next() {

		var tmp TiAuSuId
		err := rows.Scan(&tmp.Id, &tmp.Title, &tmp.Summary, &tmp.Authors)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		tmps = append(tmps, tmp)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
		return nil, err
	}

	data, err := json.MarshalIndent(tmps, " ", " ")

	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return data, nil

}

func (sqlc *SQLConn) QueryHash(hash string) (bool, error) {

	query := "select created_at from topicSummary where hash=?"
	var tmp string = ""

	rows, err := sqlc.db.Query(query, hash)

	if err != nil {
		log.Fatal(err)
		return false, err
	}
	defer rows.Close()

	for rows.Next() {

		err = rows.Scan(&tmp)
		if err != nil {
			log.Fatal(err)
			return false, err
		}
	}
	if tmp != "" {
		return true, nil
	}
	return false, nil

}
