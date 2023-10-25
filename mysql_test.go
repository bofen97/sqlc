package SQLConn

import (
	"testing"
)

/*
local mysql server

	"root:@(127.0.0.1:3306)/arxivInfo?parseTime=true"

remote mysql server

	"<username>:<passwd>@tcp(<remote>:<port>)/arxivInfo?parseTime=true"
*/
func Test_PutAllTopicGo(t *testing.T) {

	const mysqlUrl = "root:@(127.0.0.1:3306)/arxivInfo?parseTime=true"

	var sqlc = new(SQLConn)
	err := sqlc.Connect(mysqlUrl)
	if err != nil {
		t.Fatal(err)
	}
	sqlc.CreateTable()
	if err := sqlc.PutAllTopics("name for google key"); err != nil {
		t.Fatal(err)
	}
}
func Test_PutOneTopicGo(t *testing.T) {

	const mysqlUrl = "root:@(127.0.0.1:3306)/arxivInfo?parseTime=true"

	var sqlc = new(SQLConn)
	err := sqlc.Connect(mysqlUrl)
	if err != nil {
		t.Fatal(err)
	}
	sqlc.CreateTable()
	if err := sqlc.PutToTable("cs.ai"); err != nil {
		t.Fatal(err)
	}
}

func Test_Custom(t *testing.T) {

	var sqlc = new(SQLConn)
	data, err := sqlc.QueryCustomTopicFromArxiv("go programming")
	if err != nil {
		t.Fatalf("error %v", err)
	}
	t.Logf("%s\n", data)
}

func Test_Latest(t *testing.T) {
	var sqlc = new(SQLConn)
	err := sqlc.Connect("root:@(127.0.0.1:3306)/arxivInfo?parseTime=true")
	if err != nil {
		t.Error(err)
	}
	data, err := sqlc.getTopicLatestDate("cs.ai")
	if err != nil {
		t.Error(err)
	}
	t.Logf("%v", data)

}
