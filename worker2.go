package main

func (self *ReduceTask) Process2(tempdir string, client Interface) error {
	var urls []string
	//merge all output databases from the map phase. This requires downloading relevant ones
	//a reducetask's unique identifier is its self.N
	//it should have a bunch of map tasks with it's R
	//of the form `map_(self.M)_output_(self.N).db
	for idx, host := range self.SourceHosts {
		urls = append(urls, "http://"+host+"/data/"+mapOutputFile(idx, self.N))
	}
	var inpdb_path = tempdir + "/" + reduceInputFile(self.N)
	var inpdb_tmp_path = tempdir + "/" + reducePartialFile(self.N)
	var inpdb, inpdb_err = mergeDatabases(urls, inpdb_path, inpdb_tmp_path)
	if inpdb_err != nil {
		return inpdb_err
	}
	defer inpdb.Close()
	//we now have a new input database `inpdb`
	//let's create the output database...
	var outdb_path = tempdir + "/" + reduceOutputFile(self.N)
	var outdb, outdb_err = createDatabase(outdb_path)
	if outdb_err != nil {
		return outdb_err
	}
	defer outdb.Close()
	//`outdb` now also exists!
	//We need to sort some database...
	var sorted_inp, sort_err = inpdb.Query("select key, value from pairs order by key, value")
	if sort_err != nil {
		return sort_err
	}
	//`inpdb` is now sorted!! let's prepare
	//an insertion for the output...
	var insert_out, prep_err = outdb.Prepare("insert into pairs (key, value) values (?, ?)")
	if prep_err != nil {
		return prep_err
	}
	//time to go through keys sequentially. This process
	//should compare keys, close an old call to reduce,
	//and process a bunch of schtuff
	var values chan string
	var output chan Pair

	var not_first = false
	var keyCallback = func(key string) error {
		//special behaviour for not-the-first-one
		if not_first {
			close(values)
			var out = <-output
			var _, err = insert_out.Exec(out.Key, out.Value)
			if err != nil {
				return err
			}
		} else {
			not_first = true
		}
		//the first one, and all the others
		values = make(chan string)
		output = make(chan Pair) //maybe buffer these
		var err = client.Reduce(key, values, output)
		if err != nil {
			return err
		}
		return nil
	}

	var prev, key, val string
	for sorted_inp.Next() {
		var scan_err = sorted_inp.Scan(&key, &val)
		if scan_err != nil {
			return scan_err
		}
		if key != prev {
			var cb_err = keyCallback(key)
			if cb_err != nil {
				return cb_err
			}
		}
		prev = key
	}
	var trail_err = keyCallback(key)
	return trail_err
}
