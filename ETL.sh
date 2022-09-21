#! /bin/bash

path_orig=$(pwd)
dir_documents="Extracted-Data"

log(){
  local message=$1
  current_time=$(date "+%F %H-%M-%S")
  echo "$current_time,$message" >> logfile.txt  
}

extract(){
  log "Extract phase Started"
  wget wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -P "$dir_documents"
  cd $dir_documents
  tar -xvzf *.tgz; rm *.tgz
  cut -d "," -f1,2,3,4 vehicle-data.csv > extracted_csv.csv; rm vehicle-data.csv
  cut -f5-7 -d$'\t' *.tsv > extracted_tsv.csv; rm *.tsv
  cut -f6-7 -d" " *.txt > extracted_txt.csv; rm *.txt
  paste -d "," extracted_csv.csv extracted_tsv.csv extracted_txt.csv > extracted_data_consolidated.csv
  cd $path_orig
  log "Extract phase Ended"
}

transform(){
  log "Transform phase Started"
  cd $dir_documents 
  tr "[a-z]" "[A-Z]" < extracted_data_consolidated.csv > transformed_data.csv
  cd $path_orig
  log "Transform phase Ended"
}

load() {
  log "Load phase Started"
  mv $dir_documents/transformed_data.csv etl_data.csv
  log "Load phase Ended"
}

log "ETL Job Started"
extract; transform; load
log "ETL Job Ended"




