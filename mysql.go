package SQLConn

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
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
func (sqlc *SQLConn) getTopicLatestDate(topicId string) (string, error) {
	queryStr := `
	select max(published) from topicSummary where topic=? ORDER BY published DESC
	`
	queryRow := sqlc.db.QueryRow(queryStr, topicId)
	var latest string
	err := queryRow.Scan(&latest)
	if err != nil {
		return "", err
	}
	return latest, nil

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
	var wg sync.WaitGroup
	for _, topic := range arxiv.Topics {
		for _, v := range topic.SubTopics {
			wg.Add(1)

			go func(code string) {
				defer wg.Done()

				log.Printf("Put key %s into database\n", strings.ToLower(v.Code))

				sqlc.PutToTable(strings.ToLower(code))
			}(v.Code)

			time.Sleep(3 * time.Second)

		}
	}
	wg.Wait()

	return nil

}

// Query filed from db/topicSummary
type TiAuSuId struct {
	Title     string `json:"title"`
	Authors   string `json:"authors"`
	Summary   string `json:"summary"`
	Id        string `json:"url"`
	Published string `json:"published"`
}

func (sqlc *SQLConn) QueryTitleAuthorsSummaryId(topic string, date string) ([]byte, error) {
	query := "select id, title , summary,authors,published  from topicSummary where topic=? and date>=?"
	rows, err := sqlc.db.Query(query, topic, date)

	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer rows.Close()
	var tmps []TiAuSuId
	for rows.Next() {

		var tmp TiAuSuId
		err := rows.Scan(&tmp.Id, &tmp.Title, &tmp.Summary, &tmp.Authors, &tmp.Published)
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

type CustomTopicRet struct {
	Title   string `json:"title"`
	Authors string `json:"authors"`
	Summary string `json:"summary"`
	Id      string `json:"url"`
	Time    string `json:"published"`
}

func (sqlc *SQLConn) QueryCustomTopicFromArxiv(custom string) ([]byte, error) {
	var v = new(arxiv.Result)
	err := v.MakeResultFromCustomTopic(custom)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	var result []CustomTopicRet
	for _, entry := range v.Entry {

		authors := ""
		for _, author := range entry.Authors {
			authors += author.Name + ","
		}
		res := CustomTopicRet{

			Id:      entry.Id,
			Title:   entry.Title,
			Authors: authors,
			Summary: entry.Summary,

			Time: entry.Published,
		}
		result = append(result, res)
	}

	data, err := json.MarshalIndent(result, " ", " ")
	if err != nil {
		return nil, err
	}
	return data, nil
}
