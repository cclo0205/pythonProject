if [ $# = 5 ]
then
  ts=`date +%s`
  python reindex_app.py $1 $2 $3 $4 $5
  if [ $? -ne 0 ]
  then
    echo "Fail to reindex_app"
  else
    python checkApp.py $1 $2 $3 $ts
  fi
else
  echo "Use ./mainApp.sh es_host_with_port original_index_name new_index_name alias_name mapping_file_name (5 args)"

fi
