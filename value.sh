#!/bin/bash
#set -x
localhost=127.0.0.1
port=11000
if [ $1 = 'set' ];then
	if [ $# -eq 3 ];then
    	url="$localhost:$port/key"
	kv={\"$2\":\"$3\"}
    	curl -XPOST $url -d  ''$kv''
	fi
fi


if [ $1 = 'get' ];then
if [ $# -eq  2 ];then
    if [ -f tmp.html ]; then 
	rm tmp.html 
    fi	
    url="http://$localhost:$port/key/$2"
    curl -o tmp.html   "$url" >/dev/null 2>&1
    if [ -f tmp.html ]; then
        value=`cat tmp.html|head -n 1`
        echo $value
    fi
    if [ -f tmp.html ]; then
        rm tmp.html
    else
	echo nil
    fi
fi
fi

if [ $1 = 'delete' ];then
if [ $# -eq  2 ];then
    url="http://$localhost:$port/key/$2"
    curl -v -XDELETE  "$url"  >/dev/null 2>&1
    echo "delete  success"
fi
fi

