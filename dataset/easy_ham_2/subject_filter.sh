#!/bin/bash
grep -r ^Subject: . | sort | sed 's/^\(.*Subject: \)\([^\r\n]*\)/ham,\"\2\"/g' > ../easy_ham_2_subject.csv
