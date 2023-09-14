package SQLConn

import "log"

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
