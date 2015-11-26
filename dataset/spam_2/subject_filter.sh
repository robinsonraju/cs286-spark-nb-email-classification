#!/bin/bash
grep -r ^Subject: . | sort | sed 's/^\(.*Subject: \)\([^\r\n]*\)/spam,\"\2\"/g' > ../spam_2_subject.csv
