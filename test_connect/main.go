package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	// connect to DB
	//---------------------------------------------
	/*
	  In go there are a couple of packages to connect to DB.
	  One famous one is libpq
	  Another even better one by a guy jackc & it's called pgx
	  	https://github.com/jackc/pgx

	  -Install it like so in your CLI: 'go get github.com/jackc/pgx/v4'
	  	where '/v4' specifies the version to install.

	  -Obviously, you need to have installed postgres & eg a DB client like DBeaver

	  -Create a DB eg 'test_connect' in your DB system to test this with.

	  -We're going to use the default sql package which is part of go. But it's possible
	   to use different drivers for specific DB types.
	*/
	conn, err := sql.Open("pgx", "host=localhost port=5432 dbname=test_connect user=user password=")
	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to connect: %v\n", err))
	}
	defer conn.Close()

	log.Println("Conected to database")

	/*
		-If you were in production, the Open() host param will contain your remote host name
		-port 5432 is the default sql port
		-user has to be the name of your user account on your computer.
		-The 'defer' line makes sure the DB connection will be closed when it has done its job
			(THIS IS ABSOLUTELY CRUCIAL FOR PERFORMANCE)
		-Run this file to connect to the DB like so:
			//navigate to the dir with the main.go file, then run the file
			cd test_connect
			go run main.go
		-If you run it and get an  error like 'Unable to connect: sql: unknown driver "pgx" (forgotten import?)'
			it means you had to tell this package which driver you are using, so add this to your import line:

				_ "github.com/jackc/pgx/v4/stdlib"

		-When we connect to postgres using sql.Open() like this, using 'pgx', it returns a pool of DB connections
		 that we can choose from.
	*/

	// test my connection
	//---------------------------------------------
	err = conn.Ping()
	if err != nil {
		log.Fatal("Cannot connect to database!")
	}

	log.Println("Pinged database")

	// get rows from table
	//---------------------------------------------
	err = getAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}

	// insert row
	//---------------------------------------------
	// Backticks are great for writing queries coz they allow u write queries on
	// multiple lines & that can make them readable
	// we use an underscore on the 2nd line coz here we are doing an insert, so we ignore the result
	insertQuery := `INSERT INTO users (first_name, last_name) VALUES ($1, $2)`
	_, err = conn.Exec(insertQuery, "Jack", "Brown")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Inserted a row!")

	// get rows from table again (so we can notice the update from above insert query)
	//---------------------------------------------
	err = getAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}

	// update a row
	//---------------------------------------------
	updateQuery := `UPDATE users SET first_name = $1 
		WHERE id = $2`
	_, err = conn.Exec(updateQuery, "Jackie", 5)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Updated one or more rows!")

	// get rows from table again (so we can notice the update from above query)
	//---------------------------------------------
	err = getAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}

	// get one row by id
	// Notice that unline with inserts & updates where we use conn.Exec() to run the query,
	// when selecting just one row where we are sure only one row will be returned, we use
	// conn.QueryRow()
	// Also, when selecting with QueryRow, the error doesn't come with the query
	// you only get that when you try to scan (extract) the data from the returned row
	//---------------------------------------------
	fetchOneRowQuery := `SELECT id, first_name, last_name FROM users WHERE id = $1`
	var firstName, lastName string
	var id int
	row := conn.QueryRow(fetchOneRowQuery, 1)
	err = row.Scan(&id, &firstName, &lastName)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("QueryRow returns", id, firstName, lastName)

	// delete a row
	//---------------------------------------------
	deleteQuery := `DELETE FROM users WHERE id = $1`
	_, err = conn.Exec(deleteQuery, 6)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Deleted a row!")

	// get rows from table again (so we can notice the update from above delete query)
	//---------------------------------------------
	err = getAllRows(conn)
	if err != nil {
		log.Fatal(err)
	}
}

// conn is actually a pointer to sql.DB
// Just like you always need to close a connection when you connect to a DB,
// 	every time you are running a query against a DB which can return more than one row,
//	you have to close the DB connection-hence use 'defer rows.Close()' after the query line
// If you don't do this, your DB will gradually run out of resources & die in a matter of hours or daya

func getAllRows(conn *sql.DB) error {
	rows, err := conn.Query("SELECT id, first_name, last_name FROM users")
	if err != nil {
		log.Println(err)
		return err
	}
	defer rows.Close()

	var firstName, lastName string
	var id int

	for rows.Next() {
		// scan the data that you've queried from DB into your vars
		// scan them in the same order as you've queried them from the DB
		err := rows.Scan(&id, &firstName, &lastName)
		if err != nil {
			log.Println(err)
			return err
		}
		fmt.Println("Record is", id, firstName, lastName)
	}

	// check for errors again here as a safe practice-incase any error was not caught in
	// the for block above
	if err = rows.Err(); err != nil {
		log.Fatal("Error scanning rows", err)
	}

	fmt.Println("------------------------------------")

	return nil
}
